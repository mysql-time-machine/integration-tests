package com.booking.replication.it

import booking.replication.it.TransmitInsertsTest
import booking.replication.it.MicrosecondsTest
import booking.replication.it.PayloadTest
import org.junit.Test
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

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
        tests.findAll({it.does(env)}).forEach({ test -> test.doMySqlOperations(pipeline) })
        pipeline.sleep(40000) // accounts for time to startup Replicator + 30s forceFlush interval

    }

    def cleanupSpec() {
        pipeline.shutdown()
    }

    @Unroll
    def "#testName: { EXPECTED =>  #expected, RECEIVED => #received }"() {

        expect:
        expected == received

        where:
        testName << tests.findAll({it.does(env)}).collect({ test -> test.class.toString().split("\\.").last()})
        expected << tests.findAll({it.does(env)}).collect({ test -> test.getExpected(env)})
        received << tests.findAll({it.does(env)}).collect({ test -> test.getReceived(pipeline, env)})

    }
}
