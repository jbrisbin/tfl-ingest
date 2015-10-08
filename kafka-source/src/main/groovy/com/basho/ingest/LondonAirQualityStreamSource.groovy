package com.basho.ingest

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.actuate.metrics.CounterService
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Source
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.MessageChannel
import org.springframework.util.StringUtils
import org.springframework.web.bind.annotation.*
import reactor.Environment
import reactor.core.processor.RingBufferProcessor
import reactor.core.support.UUIDUtils
import reactor.jarjar.jsr166e.extra.AtomicDouble
import reactor.rx.Streams
import reactor.rx.action.Control
import reactor.rx.broadcast.Broadcaster

import java.nio.file.Files
import java.nio.file.Paths

@SpringBootApplication
@EnableBinding(Source.class)
class LondonAirQualityStreamSource {

    static {
        // Initialize Reactor's Environment (only needs to be done once somewhere in the app)
        Environment.initializeIfEmpty().assignErrorJournal()
    }

    static def HISTORICAL_DATA_ARCHIVE = "/tmp/historical_data.csv"

    def log = LoggerFactory.getLogger(LondonAirQualityStreamSource)
    def totalSize = new AtomicDouble(0.0)

    // Inject Spring Cloud Stream Kafka Source
    @Autowired
    Source out
    // Use existing Jackson ObjectMapper
    @Autowired
    ObjectMapper mapper
    @Autowired
    CounterService count
    @Autowired
    GaugeService sizeGuage

    @Bean
    List sites() {
        mapper.readValue(new URL("http://api.erg.kcl.ac.uk/AirQuality/Information/MonitoringSites/GroupName=All/Json"), Map).Sites.Site
    }

    @Bean
    Broadcaster<Map> csvOutput() {
        // Remove old file
        Files.delete(Paths.get(HISTORICAL_DATA_ARCHIVE))
        // Open new CSV file for data
        def f = new File(HISTORICAL_DATA_ARCHIVE).newOutputStream()
        // Write header line
        f.write '"site","species","timestamp","value"\n'.bytes

        // Use a Reactor Broadcaster as a file sink
        def b = Broadcaster.<Map> create(Environment.cachedDispatcher())

        b.
                filter {
                    !StringUtils.isEmpty(it['@Value'])
                }. // only write data with values
                map {
                    "\"${it['@SiteCode']}\",\"${it['@SpeciesCode']}\",\"${it['@MeasurementDateGMT']}\",\"${it['@Value']}\"\n"
                }. // extract data
                observeComplete {
                    f.flush()
                    f.close()
                }. // flush and close on exit
                consume { f.write it.bytes; f.flush() }

        b
    }

    // Once the Environment has been initialized and all services are ready, invoke this method
    Control downloadAvailableSpecies(String[] species, String from, String to, MessageChannel out) {
        // Use the RingBufferProcessor to do work in another thread
        Streams.from(sites()).
                process(RingBufferProcessor.create("mars-ingest", 512, true)).
                consume { site ->
                    // Extract site code, which is a unique code for a sensor
                    def siteCode = site['@SiteCode']
                    Streams.from(species).
                            flatMap { speciesCode ->
                                // Pull the data for a given site + species + fromDate + toDate
                                def url = "http://api.erg.kcl.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=$siteCode/SpeciesCode=$speciesCode/StartDate=$from/EndDate=$to/Json"

                                log.info "Loading data for site: ${siteCode}, species: ${speciesCode}"
                                log.info "  << $url"

                                Streams.from((List) mapper.readValue(new URL(url).openStream(), Map).RawAQData.Data).
                                        map { data ->
                                            // Add the site code and species code back into the data since it lives
                                            // at a higher level in the object hierarchy
                                            data['@SiteCode'] = siteCode
                                            data['@SpeciesCode'] = speciesCode
                                            data
                                        }
                            }.
                            consume { data ->
                                // For each data point, send a separate Message to the Spring Cloud Stream Source,
                                // which means send it to Kafka in this implementation
                                count.increment("mars.ingest.count")
                                def bytes = mapper.writeValueAsBytes(data)
                                sizeGuage.submit("mars.ingest.bytes", totalSize.addAndGet(bytes.length))

                                if (!out) {
                                    csvOutput().onNext(data)
                                } else {
                                    out.send(MessageBuilder.withPayload(bytes).build())
                                }
                            }
                }
    }

    public static void main(String[] args) {
        // Bootstrap the app
        SpringApplication.run(LondonAirQualityStreamSource)
    }

    @RestController
    static class DataDownloadController {

        def downloads = [:]

        @Autowired
        LondonAirQualityStreamSource app
        @Autowired
        Source out

        @RequestMapping(value = "/start", method = RequestMethod.GET)
        def start(
                @RequestParam(name = 'species', defaultValue = 'TMP,RHUM,SOLR,WDIR,WSPD,CO,NO2,O3,PM10,PM25,SO2') String speciesCodes,
                @RequestParam(name = 'from', defaultValue = '2015-10-01') String from,
                @RequestParam(name = 'to', defaultValue = '2015-11-01') String to,
                @RequestParam(name = 'archive', defaultValue = 'false') Boolean archive
        ) {
            def codes = speciesCodes.split(',')

            def ctl = app.downloadAvailableSpecies(codes, from, to, (archive ? null : out.output()))

            def id = UUIDUtils.create().toString()
            downloads[id] = ctl

            def hdrs = new HttpHeaders()
            hdrs.add("Link", "rel=info, href=/info/$id")
            hdrs.add("Link", "rel=stop, href=/stop/$id")
            new ResponseEntity(hdrs, HttpStatus.ACCEPTED)
        }

        @RequestMapping(value = "/info", method = RequestMethod.GET)
        def info() {
            downloads.keySet()
        }

        @RequestMapping(value = "/info/{id}", method = RequestMethod.GET)
        def info(@PathVariable String id) {
            [active: downloads[id]?.publishing ?: false]
        }

        @RequestMapping(value = "/stop/{id}", method = RequestMethod.GET)
        def stop(@PathVariable String id) {
            downloads.remove(id)?.cancel()
            info(id)
        }

    }

}
