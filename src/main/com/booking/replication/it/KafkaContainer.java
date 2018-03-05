package com.booking.replication.it;

import com.github.dockerjava.api.command.InspectContainerResponse;
import groovy.json.JsonSlurper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaContainer extends FixedHostPortGenericContainer<KafkaContainer> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaContainer.class);

    public KafkaContainer(Network network) {

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

    public ExecResult readMessagesFromKafkaTopic(String  topicName, Integer timeoutMs)
            throws IOException, InterruptedException {

        List<String> messages = new ArrayList<>();

        return this.execInContainer(
                "/opt/kafka/bin/kafka-console-consumer.sh",
                "--new-consumer",
                "--bootstrap-server", "localhost:9092",
                "--topic", topicName,
                "--timeout-ms", timeoutMs.toString(),
                "--from-beginning"
        );

    }

    public static Properties getKafkaConsumerProperties(String broker) {
        // Consumer configuration
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("group.id", "testGroup");
        prop.put("auto.offset.reset", "latest");
        prop.put("enable.auto.commit", "false");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return prop;
    }
}
