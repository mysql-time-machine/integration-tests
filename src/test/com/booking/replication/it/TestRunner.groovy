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

class TestRunner extends  Specification {

    @Shared KafkaPipeline pipeline = (new KafkaPipeline()).start()
//    @Shared HBasePipeline pipeline = (new HBasePipeline()).start()

    @Shared tests = [
            new TransmitInsertsTest()
    ]
    def setupSpec() {
        pipeline.replicator.startReplication(pipeline, "kafka")
    }

    def cleanupSpec() {
        pipeline.shutdown()
    }

    @Test
    def runTransmitInsertsTest() {

        when:
        tests.forEach({ test -> test.doMySqlOperations(pipeline) })
        pipeline.sleep(40000) // accounts for time to startup Replicator + 30s forceFlush interval

        then:
        tests.forEach({ test ->
            def expected = test.getExpected()
            def received = test.getReceived(pipeline)
            assert(expected == received)
        })

    }
}
