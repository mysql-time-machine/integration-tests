package com.booking.replication.it

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer

public class KafkaPipeline extends ReplicatorPipeline {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPipeline.class);


    public GenericContainer kafka;

    private static final Integer KAFKA_PORT = 9092;

    public KafkaPipeline() {

        super()
        outputContainer = new KafkaContainer(network)
        kafka = outputContainer

    }

    public String getKafkaIP() {
        return kafka.getContainerIpAddress();
    }

    public Integer getKafkaPort() {
        return kafka.getMappedPort(KAFKA_PORT);
    }

}