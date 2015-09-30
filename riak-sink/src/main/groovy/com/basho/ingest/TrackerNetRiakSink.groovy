package com.basho.ingest

import com.basho.riak.client.core.RiakCluster
import com.basho.riak.client.core.operations.StoreOperation
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.util.BinaryValue
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Sink
import org.springframework.context.annotation.Bean
import org.springframework.messaging.MessageHeaders
import reactor.core.support.UUIDUtils

@SpringBootApplication
@EnableBinding(Sink.class)
class TrackerNetRiakSink {

    static def LOG = LoggerFactory.getLogger(TrackerNetRiakSink)
    static def FROM_KAFKA = new Namespace("fromKafka")

    @Autowired
    RiakCluster cluster

    @Bean
    def riakSink(Sink from) {
        from.input().subscribe({ msg ->
            LOG.info("Received message: $msg")

            def loc = new Location(FROM_KAFKA, "${UUIDUtils.create()}")
            def obj = new RiakObject()
            obj.contentType = msg.headers[MessageHeaders.CONTENT_TYPE]
            obj.value = BinaryValue.create((String) msg.payload)

            LOG.info("Storing $obj at $loc")

            def op = new StoreOperation.Builder(loc).withContent(obj).build()
            cluster.execute(op)
        })
    }

    public static void main(String[] args) {
        SpringApplication.run(TrackerNetRiakSink)

        while (true) {
            Thread.sleep(5000)
        }
    }

}
