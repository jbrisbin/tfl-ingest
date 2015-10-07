package com.basho.ingest

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Source
import org.springframework.context.annotation.Bean
import org.springframework.integration.support.MessageBuilder
import reactor.Environment
import reactor.core.processor.RingBufferWorkProcessor
import reactor.rx.Stream
import reactor.rx.Streams

import java.util.concurrent.CountDownLatch

@SpringBootApplication
@EnableBinding(Source.class)
class LondonAirQualityStreamSource {

    static {
        // Initialize Reactor's Environment (only needs to be done once somewhere in the app)
        Environment.initializeIfEmpty().assignErrorJournal()
    }

    def log = LoggerFactory.getLogger(LondonAirQualityStreamSource)
    def exitLatch = new CountDownLatch(1)

    // Inject Spring Cloud Stream Kafka Source
    @Autowired
    Source out
    // Use existing Jackson ObjectMapper
    @Autowired
    ObjectMapper mapper
    // Property to use as the start date for ingestion
    @Value('${mars.ingest.from}')
    String from
    // Property to use as the end date for ingestion
    @Value('${mars.ingest.to}')
    String to
    // Which species to download
    @Value('${mars.ingest.species}')
    String speciesCodes

    // Parse the contents of the sites.json data only once since we're exposing this as a singleton Bean
    @Bean
    Stream<Map> sites() {
        Streams.from((List) mapper.readValue(new URL("http://api.erg.kcl.ac.uk/AirQuality/Information/MonitoringSites/GroupName=All/Json"), Map).Sites.Site)
    }

    // Parse the contents of the species.json data only once since we're exposing this as a singleton Bean
    @Bean
    Stream<String> species() {
        Streams.from(speciesCodes.split(','))
    }

    // Create a RingBufferWorkProcessor, which is just a multi-threaded Reactive Streams Processor, to do work
    @Bean
    RingBufferWorkProcessor workProcessor() {
        RingBufferWorkProcessor.create("mars-ingest", 4 * 1024)
    }

    // Once the Environment has been initialized and all services are ready, invoke this method
    def downloadAvailableSpecies() {
        // Use the RingBuffer to divvy up work amongst several Subscribers
        def stream = sites().process(workProcessor())

        // Subscribe to this Stream once per CPU slot
        (1..Environment.PROCESSORS).each {
            // Each Subscriber operates in its own Thread
            stream.consume { site ->
                // Extract site code, which is a unique code for a sensor
                def siteCode = site['@SiteCode']
                species().
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
                        observeComplete { exitLatch.countDown() }.
                        consume { data ->
                            // For each data point, send a separate Message to the Spring Cloud Stream Source,
                            // which means send it to Kafka in this implementation
                            out.output().send(
                                    MessageBuilder.withPayload(mapper.writeValueAsBytes(data)).build()
                            )
                        }
            }
        }
    }

    public static void main(String[] args) {
        // Bootstrap the app
        def app = SpringApplication.run(LondonAirQualityStreamSource).getBean(LondonAirQualityStreamSource)

        // Everything's ready, go do work
        app.downloadAvailableSpecies()

        // Stay active until through processing
        app.exitLatch.await()
    }

}
