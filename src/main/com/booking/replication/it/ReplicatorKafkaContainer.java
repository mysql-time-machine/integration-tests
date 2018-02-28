package com.booking.replication.it;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.List;

public class ReplicatorKafkaContainer extends FixedHostPortGenericContainer<ReplicatorKafkaContainer> {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorKafkaContainer.class);

    public ReplicatorKafkaContainer(Network network) {

        super("wurstmeister/kafka:1.0.0");
        withNetwork(network);
        withNetworkAliases("kafka");
        withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181");
        withEnv("KAFKA_CREATE_TOPICS", "replicator_test_kafka:1:1,replicator_validation:1:1");
        withExposedPorts(9092);

    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        logger.info("kafka container started at { ip: " + getContainerIpAddress() + ", port: " + getMappedPort(9092));
    }
}
