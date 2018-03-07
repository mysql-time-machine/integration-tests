package com.booking.replication.it

import booking.replication.it.TransmitInsertsTest
import booking.replication.it.MicrosecondsTest
import booking.replication.it.PayloadTest
import org.junit.Test
import spock.lang.Shared
import spock.lang.Specification

@GrabConfig(systemClassLoader = true)

class TestRunner extends  Specification {

    @Shared env = "hbase"

    @Shared ReplicatorPipeline pipeline = PipelineFactory.getPipeline(env).start()

    @Shared tests = [
            new TransmitInsertsTest(),
            new MicrosecondsTest(),
            new PayloadTest()
    ]
    def setupSpec() {
        pipeline.replicator.startReplication(pipeline, env)
    }

    def cleanupSpec() {
        pipeline.shutdown()
    }

    @Test
    def runAllTests() {

        setup:
        def local_tests = tests.findAll({it.does(env)})

        when:
        local_tests.forEach({ test -> test.doMySqlOperations(pipeline) })
        pipeline.sleep(40000) // accounts for time to startup Replicator + 30s forceFlush interval

        then:
        local_tests.forEach({ test ->
            def expected = test.getExpected(env)
            def received = test.getReceived(pipeline, env)
            assert(expected == received)
        })

    }
}
