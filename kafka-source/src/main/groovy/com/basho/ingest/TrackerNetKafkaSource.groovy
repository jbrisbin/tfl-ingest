package com.basho.ingest

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Source
import org.springframework.context.annotation.Bean
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.MessageHeaders
import reactor.Environment
import reactor.io.net.NetStreams
import reactor.rx.Streams

import java.util.concurrent.TimeUnit

@SpringBootApplication
@EnableBinding(Source.class)
class TrackerNetKafkaSource {

    static {
        Environment.initializeIfEmpty().assignErrorJournal()
    }

    static def TFL_URL = "http://cloud.tfl.gov.uk/TrackerNet/PredictionSummary/C"

    @Autowired
    Source out

    @Bean
    def trackerNetPredictionSummary() {
        Streams.period(0, 30, TimeUnit.SECONDS).
                flatMap {
                    NetStreams.httpClient().
                            get(TFL_URL).
                            flatMap { XmlSlurper.parse(it) }
                }.
                flatMap {
                    Streams.from(it.S)
                }.
                consume { station ->
                    def statusMsg = [
                            name     : station.@N.text(),
                            platforms: station.P.collect { platform ->
                                [name  : platform.@N.text(),
                                 trains: platform.T.collect { train ->
                                     [timeToStation: train.@C.text(),
                                      destination  : train.@DE.text(),
                                      location     : train.@L.text()]
                                 }]
                            }
                    ]
                    out.output().send(
                            MessageBuilder.withPayload(statusMsg.toJSON()).
                                    setHeader(MessageHeaders.CONTENT_TYPE, "application/json").
                                    build()
                    )
                }
    }

    public static void main(String[] args) {
        SpringApplication.run(TrackerNetKafkaSource)

        while (true) {
            Thread.sleep(5000)
        }
    }

}
