package com.booking.replication.it

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

@GrabConfig(systemClassLoader = true)

class TestRunnerIT extends Specification {

    @Shared env = "hbase"

    @Shared ReplicatorPipeline pipeline = PipelineFactory.getPipeline(env).start()

    @Shared tests = [
            new TransmitInsertsTest(),
            new MicrosecondsTest(),
            new PayloadTest(),
            new LongTransactionHBaseTest()
    ]

    def setupSpec() {

        // v014 branch
        //pipeline.replicator.startReplicationFromFirstBinlogFile_V0145(pipeline, env)

        // v015 branch
      pipeline.replicator.startReplicationFromFirstBinlogFile_V015(pipeline, env, "bc")

        tests.findAll({it.does(env)}).forEach({ test -> test.doMySqlOperations(pipeline) })
        pipeline.sleep(40000) // accounts for time to startup Replicator + 30s forceFlush interval

    }

    def cleanupSpec() {
        //pipeline.sleep(10000000)
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
