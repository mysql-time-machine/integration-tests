package com.booking.replication.it

abstract class ReplicatorTest {
    abstract boolean does(String env)
    abstract ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline)
    abstract List<String> getExpected(String env)
    abstract List<String> getReceived(ReplicatorPipeline pipeline, String env)

    List parseHBase(String result) {
        def cells = result.split("\n").drop(1).dropRight(3).collect({ (it =~ / (.*) column=(.*), timestamp=(\d+), value=(.*)/)[0] })
        return cells
    }
}
