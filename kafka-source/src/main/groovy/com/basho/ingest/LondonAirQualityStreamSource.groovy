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
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.integration.support.MessageBuilder
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.Environment
import reactor.core.processor.RingBufferProcessor
import reactor.core.support.UUIDUtils
import reactor.jarjar.jsr166e.extra.AtomicDouble
import reactor.rx.Streams
import reactor.rx.action.Control

@SpringBootApplication
@EnableBinding(Source.class)
class LondonAirQualityStreamSource {

    static {
        // Initialize Reactor's Environment (only needs to be done once somewhere in the app)
        Environment.initializeIfEmpty().assignErrorJournal()
    }

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

    // Once the Environment has been initialized and all services are ready, invoke this method
    Control downloadAvailableSpecies(String[] species, String from, String to) {
        // Use the RingBufferProcessor to do work in another thread
        Streams.from(sites()).
                process(RingBufferProcessor.create("mars-ingest", 1024, true)).
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
                                out.output().send(MessageBuilder.withPayload(bytes).build())
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

        @RequestMapping(value = "/start", method = RequestMethod.GET)
        def start(
                @RequestParam(name = 'species', defaultValue = 'TMP,RHUM,SOLR,WDIR,WSPD,CO,NO2,O3,PM10,PM25,SO2') String speciesCodes,
                @RequestParam(name = 'from', defaultValue = '2015-10-01') String from,
                @RequestParam(name = 'to', defaultValue = '2015-11-01') String to
        ) {
            def codes = speciesCodes.split(',')

            def ctl = app.downloadAvailableSpecies(codes, from, to)

            def id = UUIDUtils.create().toString()
            downloads[id] = ctl

            new ResponseEntity(new LinkedMultiValueMap([
                    Link: ["rel=info,href=/info/$id".toString(), "rel=stop,href=/stop/$id".toString()]
            ]), HttpStatus.ACCEPTED)
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
