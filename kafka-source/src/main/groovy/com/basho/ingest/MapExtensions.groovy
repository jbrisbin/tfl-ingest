package com.basho.ingest

import com.basho.riak.client.core.RiakFuture
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic
import reactor.core.reactivestreams.SubscriberWithContext
import reactor.rx.Stream
import reactor.rx.Streams

/**
 * Created by jbrisbin on 9/30/15.
 */
@CompileStatic
class MapExtensions {

    static ObjectMapper mapper = new ObjectMapper()

    // Helper method to write out a Map as a String of JSON
    static String toJSON(Map selfType) {
        mapper.writeValueAsString(selfType)
    }

}
