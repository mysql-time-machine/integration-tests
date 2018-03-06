package com.booking.replication.it

abstract class ReplicatorTest {
    abstract boolean does(String env)
    abstract ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline)
    abstract List<String> getExpected(String env)
    abstract List<String> getReceived(ReplicatorPipeline pipeline, String env)
}
