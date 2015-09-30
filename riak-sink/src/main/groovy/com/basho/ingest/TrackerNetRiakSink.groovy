package com.basho.ingest

import com.basho.riak.client.core.RiakCluster
import com.basho.riak.client.core.operations.DeleteOperation
import com.basho.riak.client.core.operations.FetchOperation
import com.basho.riak.client.core.operations.ListKeysOperation
import com.basho.riak.client.core.operations.StoreOperation
import com.basho.riak.client.core.query.Location
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.util.BinaryValue
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Sink
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.messaging.MessageHeaders
import org.springframework.web.bind.annotation.*
import org.springframework.web.context.request.async.DeferredResult
import reactor.Environment
import reactor.core.reactivestreams.SubscriberWithContext
import reactor.core.support.UUIDUtils
import reactor.rx.Stream
import reactor.rx.Streams

@SpringBootApplication
@EnableBinding(Sink.class)
class TrackerNetRiakSink {

    static {
        Environment.initializeIfEmpty().assignErrorJournal()
    }

    static def LOG = LoggerFactory.getLogger(TrackerNetRiakSink)

    @Autowired
    RiakCluster cluster
    @Autowired
    Sink from

    @Bean
    Namespace fromKafkaNamespace() {
        new Namespace("fromKafka")
    }

    @Bean
    def riakSink() {
        from.input().subscribe({ msg ->
            def loc = new Location(fromKafkaNamespace(), "${UUIDUtils.create()}")
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

    @RestController
    static class AdminController {

        @Autowired
        RiakCluster cluster
        @Autowired
        Namespace fromKafkaNamespace
        @Autowired
        ObjectMapper mapper

        @RequestMapping(method = RequestMethod.GET, produces = "application/json")
        @ResponseBody
        def list() {
            def res = new DeferredResult()
            listKeys().
                    buffer().
                    consume {
                        res.result = it.collect { it.toStringUtf8() }
                    }
            res
        }

        @RequestMapping(method = RequestMethod.DELETE)
        DeferredResult<ResponseEntity> clear() {
            def res = new DeferredResult<ResponseEntity>()
            listKeys().
                    map {
                        new Location(fromKafkaNamespace, it)
                    }.
                    consume {
                        cluster.execute(new DeleteOperation.Builder(it).build())
                    }
            res.result = new ResponseEntity(HttpStatus.ACCEPTED)
            res
        }

        @RequestMapping(value = "/{key}", method = RequestMethod.GET, produces = "application/json")
        @ResponseBody
        def summary(@PathVariable String key) {
            def res = new DeferredResult()
            fetchSummary(key).
                    buffer().
                    consume {
                        res.result = it.collect { mapper.readValue(it.value.value, Map) }
                    }
            res
        }

        private Stream<BinaryValue> listKeys() {
            def f = cluster.execute(new ListKeysOperation.Builder(fromKafkaNamespace).build())
            Streams.createWith({ Long req, SubscriberWithContext<BinaryValue, Void> sub ->
                f.addListener {
                    it.get().keys.collect { sub.onNext(it) }
                    sub.onComplete()
                }
            })
        }

        private Stream<RiakObject> fetchSummary(String key) {
            def f = cluster.execute(new FetchOperation.Builder(new Location(fromKafkaNamespace, key)).build())
            Streams.createWith({ Long req, SubscriberWithContext<RiakObject, Void> sub ->
                f.addListener {
                    it.get().objectList.collect { sub.onNext(it) }
                    sub.onComplete()
                }
            })
        }

    }

}
