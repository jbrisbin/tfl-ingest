package com.basho.ingest

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.cloud.stream.messaging.Source
import org.springframework.integration.support.MessageBuilder
import org.springframework.stereotype.Service
import reactor.rx.Stream
import reactor.rx.Streams
import reactor.rx.action.Control

/**
 * Created by jbrisbin on 10/6/15.
 */
@Service
class AirQualitySpeciesDownloader {

    final Stream<Map> sites
    final Stream<Map> species
    final Source out
    final ObjectMapper mapper

    @Autowired
    AirQualitySpeciesDownloader(Stream<Map> sites,
                                Stream<Map> species,
                                Source out,
                                ObjectMapper mapper) {
        this.sites = sites
        this.species = species
        this.out = out
        this.mapper = mapper
    }

    Control loadRange(String from, String to) {
        sites.consume { site ->
            def siteCode = site['@SiteCode']
            species.
                    flatMap { species ->
                        def speciesCode = species['@SpeciesCode']
                        Streams.from(mapper.readValue(new URL("http://api.erg.kcl.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=$siteCode/SpeciesCode=$speciesCode/StartDate=$from/EndDate=$to/Json").openStream(), Map).RawAQData.Data).
                                map { data ->
                                    data['@SiteCode'] = siteCode
                                    data['@SpeciesCode'] = speciesCode
                                    data
                                }
                    }.
                    consume { data ->
                        out.output().send(
                                MessageBuilder.withPayload(data.toJSON()).build()
                        )
                    }
        }
    }

}
