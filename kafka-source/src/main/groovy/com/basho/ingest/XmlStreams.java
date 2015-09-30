package com.basho.ingest;

import groovy.lang.Closure;
import groovy.util.XmlSlurper;
import groovy.util.slurpersupport.GPathResult;
import reactor.io.buffer.Buffer;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.io.ByteArrayInputStream;

/**
 * Created by jbrisbin on 9/30/15.
 */
public class XmlStreams {

    public static Stream<GPathResult> from(Buffer buffer, Closure cl) {
        try {
            GPathResult res = new XmlSlurper().parse(new ByteArrayInputStream(buffer.asBytes()));
            if (null != cl) {
                Object o = cl.call(res);
                if (o instanceof GPathResult) {
                    return Streams.from((GPathResult) o);
                } else {
                    return Streams.empty();
                }
            } else {
                return Streams.just(res);
            }
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

}
