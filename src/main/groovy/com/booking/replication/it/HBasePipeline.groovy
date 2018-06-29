package com.booking.replication.it

import org.testcontainers.containers.GenericContainer

class HBasePipeline extends ReplicatorPipeline {
    public GenericContainer kafka;

    public HBasePipeline() {

        super()
        outputContainer = new com.booking.replication.it.HBaseContainer(network)

    }
}