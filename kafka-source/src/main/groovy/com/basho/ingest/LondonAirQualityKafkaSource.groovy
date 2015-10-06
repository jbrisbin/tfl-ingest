package com.basho.ingest

import org.slf4j.Logger
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
import reactor.rx.Stream
import reactor.rx.Streams

@SpringBootApplication
@EnableBinding(Source.class)
class LondonAirQualityKafkaSource {

    static {
        Environment.initializeIfEmpty().assignErrorJournal()
    }

    Logger log = LoggerFactory.getLogger(LondonAirQualityKafkaSource)

    @Autowired
    Source out
    @Value('${mars.ingest.from}')
    String from
    @Value('${mars.ingest.to}')
    String to

    @Bean
    Stream<Map> sites() {
        Streams.from(Map.parse("sites.json").Sites.Site)
    }

    @Bean
    Stream<Map> species() {
        Streams.from(Map.parse("species.json").AirQualitySpecies.Species)
    }

    def downloadAvailableSpecies() {
        sites().consume { site ->
            def siteCode = site['@SiteCode']
            species().
                    flatMap { species ->
                        def speciesCode = species['@SpeciesCode']

                        log.info "Loading data for site: ${siteCode}, species: ${speciesCode}"

                        Streams.from(Map.parse(new URL("http://api.erg.kcl.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=$siteCode/SpeciesCode=$speciesCode/StartDate=$from/EndDate=$to/Json")).RawAQData.Data).
                                map { data ->
                                    data['@SiteCode'] = siteCode
                                    data['@SpeciesCode'] = speciesCode
                                    data
                                }
                    }.
                    consume { data ->
                        out.output().send(
                                MessageBuilder.withPayload(data.toJSON()).build()
                        )
                    }
        }
    }

    public static void main(String[] args) {
        def app = SpringApplication.run(LondonAirQualityKafkaSource).getBean(LondonAirQualityKafkaSource)

        app.downloadAvailableSpecies()

        while (true) {
            Thread.sleep(5000)
        }
    }

}
