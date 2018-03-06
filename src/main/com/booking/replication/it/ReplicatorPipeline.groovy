package com.booking.replication.it

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network

public class ReplicatorPipeline {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPipeline.class);

    public Network network;

    public GenericContainer mysql;
    public GenericContainer zookeeper;
    public GenericContainer replicator;
    public GenericContainer graphite;

    protected Thread replicatorCmdHandle;

    private static final Integer KAFKA_PORT = 9092;
    private static final Integer ZOOKEEPER_PORT = 2181;

    public ReplicatorPipeline() {

        network = Network.newNetwork();

        mysql = new MySqlContainer(network,"mysql:5.6.27");

        zookeeper = new GenericContainer("zookeeper:3.4")
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withExposedPorts(ZOOKEEPER_PORT)
        ;

        graphite = new GenericContainer("hopsoft/graphite-statsd:latest")
                .withNetwork(network)
                .withNetworkAliases("graphite")
                .withExposedPorts(80)
        ;

        replicator = new ReplicatorContainer("replicator-runner:latest", network)

    }

    public ReplicatorPipeline sleep(long ms) {
        logger.info("Sleep for " + ms + " ms...")
        Thread.sleep(ms);
        return this
    }

    public ReplicatorPipeline start() {
        mysql.start();
        zookeeper.start();
        graphite.start();
        replicator.start();

        return this;
    }

    public void shutdown() {

        replicator.stop();
        graphite.stop();
        zookeeper.stop();
        mysql.stop();

    }

    public String getMySqlIP() {
        return mysql.getContainerIpAddress();
    }

    public Integer getMySqlPort() {
        return mysql.getMappedPort(3306);
    }


    public String getGraphitelIP() {
        return graphite.getContainerIpAddress();
    }

    public Integer getGraphitePort() {
        return graphite.getMappedPort(80);
    }

}