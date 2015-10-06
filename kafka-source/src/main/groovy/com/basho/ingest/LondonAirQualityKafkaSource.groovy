package com.basho.ingest

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Source
import org.springframework.context.annotation.Bean
import reactor.Environment
import reactor.rx.Stream
import reactor.rx.Streams

@SpringBootApplication
@ConfigurationProperties
@EnableBinding(Source.class)
class LondonAirQualityKafkaSource {

    static {
        Environment.initializeIfEmpty().assignErrorJournal()
    }

    @Autowired
    ObjectMapper mapper
    @Value('${mars.ingest.from}')
    String from
    @Value('${mars.ingest.to}')
    String to

    @Bean
    Stream<Map> sites() {
        Streams.from((List) read("sites.json").Sites.Site)
    }

    @Bean
    Stream<Map> species() {
        Streams.from((List) read("species.json").AirQualitySpecies.Species)
    }

    Map read(String resource) {
        mapper.readValue(LondonAirQualityKafkaSource.classLoader.getResource(resource), Map)
    }

    public static void main(String[] args) {
        def ctx = SpringApplication.run(LondonAirQualityKafkaSource)

        def app = ctx.getBean(LondonAirQualityKafkaSource)
        def dl = ctx.getBean(AirQualitySpeciesDownloader)

        def log = LoggerFactory.getLogger(LondonAirQualityKafkaSource)

        log.info("Loading data for range ${app.from} to ${app.to}")
        dl.loadRange(app.from, app.to)

        while (true) {
            Thread.sleep(5000)
        }
    }

}
