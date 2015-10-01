package com.basho.ingest

import groovy.transform.CompileStatic
import groovy.util.slurpersupport.GPathResult
import org.reactivestreams.Publisher
import reactor.core.reactivestreams.SubscriberWithContext
import reactor.io.buffer.Buffer
import reactor.rx.Stream
import reactor.rx.Streams

/**
 * Created by jbrisbin on 9/30/15.
 */
@CompileStatic
class XmlSlurperStaticExtensions {

    static <P extends Publisher<Buffer>> Stream<GPathResult> parse(XmlSlurper selfType, P p) {
        Streams.createWith(
                { Long req, SubscriberWithContext<GPathResult, Buffer> sub ->
                    Streams.wrap(p).
                            observeComplete {
                                sub.context().flip()
                                sub.onNext(new XmlSlurper().parse(new ByteArrayInputStream(sub.context().asBytes())))
                                sub.onComplete()
                            }.
                            consume { Buffer buf -> sub.context().append(buf) }
                },
                {
                    new Buffer()
                }
        )
    }

}
