package com.booking.replication.it

class PipelineFactory {
    static ReplicatorPipeline getPipeline(String env) {
        if (env.equals("kafka")) {
            return new KafkaPipeline()
        } else if (env.equals("hbase")) {
            return new HBasePipeline()
        } else {
            throw new RuntimeException(env + " is not known as environment")
        }
    }
}
