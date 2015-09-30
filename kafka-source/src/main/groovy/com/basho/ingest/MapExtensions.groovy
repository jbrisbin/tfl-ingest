package com.basho.ingest

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic

/**
 * Created by jbrisbin on 9/30/15.
 */
@CompileStatic
class MapExtensions {

    static ObjectMapper mapper = new ObjectMapper()

    static String toJSON(Map selfType) {
        mapper.writeValueAsString(selfType)
    }

}
