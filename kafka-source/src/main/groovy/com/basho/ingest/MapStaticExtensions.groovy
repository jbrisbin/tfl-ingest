package com.basho.ingest

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic

/**
 * Created by jbrisbin on 10/6/15.
 */
@CompileStatic
class MapStaticExtensions {

    static ObjectMapper mapper = new ObjectMapper()

    // Helper method to parse a remote URL of JSON into a Map
    static Map parse(Map selfType, URL url) {
        mapper.readValue(url.openStream(), Map)
    }

    // Helper method to parse a classpath resource of JSON into a Map
    static Map parse(Map selfType, String resource) {
        mapper.readValue(MapStaticExtensions.classLoader.getResourceAsStream(resource), Map)
    }

}
