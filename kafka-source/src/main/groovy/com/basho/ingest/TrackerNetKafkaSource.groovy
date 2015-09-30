package com.basho.ingest

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Source
import org.springframework.context.annotation.Bean
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.json.ObjectToJsonTransformer
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.MessageChannel
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

    @Bean
    def stationStatusMessages() {
        new DirectChannel()
    }

    @Bean
    def trackerNetPredictionSummary(MessageChannel stationStatusMessages) {
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
                    stationStatusMessages.send(MessageBuilder.withPayload(statusMsg).build())
                }
    }

    @Bean
    def kafkaSink(MessageChannel stationStatusMessages, Source out) {
        IntegrationFlows.from(stationStatusMessages)
                .transform(new ObjectToJsonTransformer())
                .channel(out.output())
                .get()
    }

    public static void main(String[] args) {
        SpringApplication.run(TrackerNetKafkaSource)

        while (true) {
            Thread.sleep(5000)
        }
    }

}
