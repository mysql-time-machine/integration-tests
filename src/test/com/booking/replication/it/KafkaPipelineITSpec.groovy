package com.booking.replication.it

import groovy.json.JsonSlurper

import org.junit.rules.*

import org.slf4j.Logger;
import org.slf4j.LoggerFactory

import spock.lang.Unroll
import spock.lang.Shared
import spock.lang.Specification

@GrabConfig(systemClassLoader = true)
import groovy.sql.Sql

import static groovy.json.JsonOutput.*

class KafkaPipelineITSpec extends  Specification {

    @Shared KafkaPipeline pipeline = (new KafkaPipeline()).start()

    def cleanupSpec() {
        pipeline.shutdown()
    }

    @Unroll
    def "pipelineTransmitRows{#result == #expected}"()  {

        expect:
        result == expected

        where:
        result << pipeline
                    .sleep(10000)
                    .InsertTestRowsToMySQL()
                    .sleep(10000)
                    .startReplication() // TODO: add wait for readiness check
                    .sleep(60000)
                    .readRowsFromKafka().collect{
                        it.get(0) + "|" +
                        it.get(1) + "|" +
                        it.get(2) + "|" +
                        it.get(3)
                    }
        expected << [
                "A|1|665726|PZBAAQSVoSxxFassEAQ",
                "B|2|49070|cvjIXQiWLegvLs kXaKH",
                "C|3|437616|pjFNkiZExAiHkKiJePMp"
        ]
    }
}
