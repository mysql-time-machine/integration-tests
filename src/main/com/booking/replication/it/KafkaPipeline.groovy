package com.booking.replication.it

import groovy.json.JsonSlurper
import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.images.RemoteDockerImage

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson


public class KafkaPipeline extends ReplicatorPipeline {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPipeline.class);


    public GenericContainer kafka;

    private static final Integer KAFKA_PORT = 9092;

    public KafkaPipeline() {

        super()
        kafka = new KafkaContainer(network);

    }

    public KafkaPipeline start() {
        super.start();
        kafka.start();

        return this;
    }

    public void shutdown() {
        super.shutdown()
        kafka.stop();

    }

    public String getKafkaIP() {
        return kafka.getContainerIpAddress();
    }

    public Integer getKafkaPort() {
        return kafka.getMappedPort(KAFKA_PORT);
    }

}