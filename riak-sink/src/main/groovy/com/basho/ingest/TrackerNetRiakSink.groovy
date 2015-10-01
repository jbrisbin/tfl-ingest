package com.basho.ingest

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.DeleteValue
import com.basho.riak.client.api.commands.kv.FetchValue
import com.basho.riak.client.api.commands.kv.ListKeys
import com.basho.riak.client.api.commands.kv.StoreValue
import com.basho.riak.client.core.RiakFuture
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
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
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
    Sink from
    @Autowired
    RiakClient riakClient

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

            riakClient.execute(new StoreValue.Builder(obj).withLocation(loc).build())
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
        RiakClient riakClient
        @Autowired
        Namespace fromKafkaNamespace
        @Autowired
        ObjectMapper mapper

        @RequestMapping(method = RequestMethod.GET, produces = "application/json")
        def list() {
            def res = new DeferredResult()
            listKeys().
                    buffer().
                    observeComplete {
                        if (!res.isSetOrExpired()) res.result = []
                    }.
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
                    observeComplete {
                        res.result = new ResponseEntity(HttpStatus.ACCEPTED)
                    }.
                    consume {
                        riakClient.execute(new DeleteValue.Builder(it).build())
                    }
            res
        }

        @RequestMapping(value = "/{key}", method = RequestMethod.GET, produces = "application/json")
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
            Streams.createWith(
                    { Long req, SubscriberWithContext<BinaryValue, RiakFuture> sub ->
                        sub.context().addListener { f ->
                            f.get().keys?.each { sub.onNext(it) }
                            sub.onComplete()
                        }
                    },
                    {
                        riakClient.executeAsync(new ListKeys.Builder(fromKafkaNamespace).build())
                    }
            )
        }

        private Stream<RiakObject> fetchSummary(String key) {
            Streams.createWith(
                    { Long req, SubscriberWithContext<RiakObject, RiakFuture> sub ->
                        sub.context().addListener { f ->
                            f.get().values?.each { sub.onNext(it) }
                            sub.onComplete()
                        }
                    },
                    {
                        riakClient.executeAsync(new FetchValue.Builder(new Location(fromKafkaNamespace, key)).build())
                    }
            )
        }

    }

}
