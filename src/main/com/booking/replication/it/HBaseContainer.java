package com.booking.replication.it;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseContainer extends FixedHostPortGenericContainer<KafkaContainer> {
    public HBaseContainer(Network network) {

        super("harisekhon/hbase:latest");
        withNetwork(network);
        withNetworkAliases("hbase");
    }

    public ExecResult readMessagesFromKafkaTopic(String  topicName, Integer timeoutMs)
            throws IOException, InterruptedException {

        return new ExecResult("stdout mock", "stderr mock");

    }
}
