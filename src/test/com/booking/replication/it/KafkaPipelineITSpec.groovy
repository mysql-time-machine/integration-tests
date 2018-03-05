package com.booking.replication.it

import booking.replication.it.TransmitInsertsTest
import groovy.json.JsonSlurper
import org.junit.Test
import org.junit.rules.*

import org.slf4j.Logger;
import org.slf4j.LoggerFactory

import spock.lang.Unroll
import spock.lang.Shared
import spock.lang.Specification

@GrabConfig(systemClassLoader = true)
import groovy.sql.Sql
import sun.jvm.hotspot.runtime.Thread

import static groovy.json.JsonOutput.*

class KafkaPipelineITSpec extends  Specification {

    @Shared KafkaPipeline pipeline = (new KafkaPipeline()).start()

    @Shared tests = [
            new TransmitInsertsTest()
    ]

    def setupSpec() {
        pipeline.startReplication()
    }

    def cleanupSpec() {
        pipeline.shutdown()
    }

    @Unroll
    def "pipelineTransmitRows{#result == #expected}"()  {

        expect:
        result == expected

        where:
        result << pipeline
                    .exec({
                        sleep(10000)
                    })

                    .exec({
                        tests.forEach{ test -> test.doMySqlOperations(pipeline) }
                    })

                    .sleep(60000)   // accounts for time to startup Replicator + 30s forceFlush interval

                    .readRowsFromKafka().collect{
                        it.get(0) + "|" +
                        it.get(1) + "|" +
                        it.get(2) + "|" +
                        it.get(3)
                    }
        expected << [
                "A|1|665726|PZBAAQSVoSxxFassQEAQ",
                "B|2|490705|cvjIXQiWLegvLs kXaKH",
                "C|3|437616|pjFNkiZExAiHkKiJePMp",
                "D|4|537616|SjFNkiZExAiHkKiJePMp",
                "E|5|637616|ajFNkiZExAiHkKiJePMp"
        ]
    }
}