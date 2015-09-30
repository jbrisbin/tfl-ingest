package com.basho.ingest

import groovy.transform.CompileStatic
import groovy.util.slurpersupport.GPathResult
import org.reactivestreams.Publisher
import reactor.core.reactivestreams.SubscriberWithContext
import reactor.fn.BiConsumer
import reactor.io.buffer.Buffer
import reactor.rx.Stream
import reactor.rx.Streams

/**
 * Created by jbrisbin on 9/30/15.
 */
@CompileStatic
class XmlSlurperStaticExtensions {

    static <P extends Publisher<Buffer>> Stream<GPathResult> parse(XmlSlurper selfType, P p) {
        def x = new XmlSlurper()
        def b = new Buffer()
        Streams.createWith({ Long req, SubscriberWithContext<GPathResult, Void> sub ->
            def onComplete = {
                b.flip()
                sub.onNext(x.parse(new ByteArrayInputStream(b.asBytes())))
            }
            Streams.wrap(p).
                    observeComplete(onComplete).
                    consume { Buffer buf -> b.append(buf) }
        } as BiConsumer<Long, SubscriberWithContext<GPathResult, Void>>)
    }

}
