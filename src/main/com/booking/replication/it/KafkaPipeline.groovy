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


public class KafkaPipeline {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPipeline.class);

    public Network network;

    public GenericContainer mysql;
    public GenericContainer zookeeper;
    public GenericContainer kafka;
    public GenericContainer replicator;
    public GenericContainer graphite;

    private Thread replicatorCmdHandle;

    private static final Integer KAFKA_PORT = 9092;
    private static final Integer ZOOKEEPER_PORT = 2181;

    public KafkaPipeline() {

        network = Network.newNetwork();

        mysql = new MySqlContainer(network,"mysql:5.6.27");

        zookeeper = new GenericContainer("zookeeper:3.4")
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withExposedPorts(ZOOKEEPER_PORT)
        ;

        kafka = new KafkaContainer(network);

        graphite = new GenericContainer("hopsoft/graphite-statsd:latest")
                .withNetwork(network)
                .withNetworkAliases("graphite")
                .withExposedPorts(80)
        ;

        replicator = new ReplicatorContainer("replicator-runner:latest", network)

    }

    public KafkaPipeline sleep(long ms) {
        logger.info("Sleep for " + ms + " ms...")
        Thread.sleep(ms);
        return this
    }

    public KafkaPipeline start() {
        mysql.start();
        zookeeper.start();
        kafka.start();
        graphite.start();
        replicator.start();
        return this;
    }

    public void shutdown() {

        replicator.stop();
        // replicatorCmdHandle.join();

        graphite.stop();
        kafka.stop();
        zookeeper.stop();
        mysql.stop();

    }

    public String getMySqlIP() {
        return mysql.getContainerIpAddress();
    }

    public Integer getMySqlPort() {
        return mysql.getMappedPort(3306);
    }

    public String getKafkaIP() {
        return kafka.getContainerIpAddress();
    }

    public Integer getKafkaPort() {
        return kafka.getMappedPort(KAFKA_PORT);
    }


    public String getGraphitelIP() {
        return graphite.getContainerIpAddress();
    }

    public Integer getGraphitePort() {
        return graphite.getMappedPort(80);
    }


    // TODO: move to tests
    def readRowsFromKafka() {

        def allRows = []

        def result = kafka.readMessagesFromKafkaTopic("replicator_test_kafka",10000);

        def messages = result.getStdout()

        def jsonSlurper = new JsonSlurper()

        messages.eachLine { line ->

            logger.debug("message => " + line.toString())

            def messageEntries = jsonSlurper.parseText(line)

            def inserts =
                    messageEntries['rows'].findAll {
                        it["eventType"] == "INSERT"
                    }

            def rows = inserts.collect {
                [
                        it["eventColumns"]["pk_part_1"]["value"],
                        it["eventColumns"]["pk_part_2"]["value"],
                        it["eventColumns"]["randomint"]["value"],
                        it["eventColumns"]["randomvarchar"]["value"]
                ]
            }

            rows.each{ row -> allRows.add(row) }
        }

        return allRows;
    }

    public KafkaPipeline exec(func) {
        func.call()
        return this;
    }

//    // TODO: move to tests
//    public KafkaPipeline InsertTestRowsToMySQL() {
//
//        def replicant = mysql.getReplicantSql()
//        def activeSchema = mysql.getActiveSchemaSql()
//
//        // CREATE
//        def sqlCreate = """
//CREATE TABLE IF NOT EXISTS
//      sometable (
//      pk_part_1         varchar(5) NOT NULL DEFAULT '',
//      pk_part_2         int(11)    NOT NULL DEFAULT 0,
//      randomInt         int(11)             DEFAULT NULL,
//      randomVarchar     varchar(32)         DEFAULT NULL,
//      PRIMARY KEY       (pk_part_1,pk_part_2),
//      KEY randomVarchar (randomVarchar),
//      KEY randomInt     (randomInt)
//    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
//"""
//
//        replicant.execute(sqlCreate);
//        replicant.commit();
//
//        activeSchema.execute(sqlCreate);
//        activeSchema.commit();
//
//        replicant.execute("reset master")
//        activeSchema.execute("reset master")
//
//        // INSERT
//        def testRows = [
//                ['A', '1', '665726', 'PZBAAQSVoSxxFassQEAQ'],
//                ['B', '2', '490705', 'cvjIXQiWLegvLs kXaKH'],
//                ['C', '3', '437616', 'pjFNkiZExAiHkKiJePMp'],
//                ['D', '4', '537616', 'SjFNkiZExAiHkKiJePMp'],
//                ['E', '5', '637616', 'ajFNkiZExAiHkKiJePMp']
//        ]
//
//        testRows.each {
//            row ->
//                try {
//                    def sqlString = """
//                        INSERT INTO
//                            sometable (
//                                pk_part_1,
//                                pk_part_2,
//                                randomInt,
//                                randomVarchar
//                            )
//                            values (
//                                ${row[0]},
//                                ${row[1]},
//                                ${row[2]},
//                                ${row[3]}
//                            )
//                    """
//
//                    replicant.execute(sqlString)
//                    replicant.commit()
//                } catch (Exception ex) {
//                    replicant.rollback()
//                }
//        }
//
//        // SELECT CHECK
//        def resultSet = []
//        replicant.eachRow('select * from sometable') {
//            row ->
//                resultSet.add([
//                        pk_part_1    : row.pk_part_1,
//                        pk_part_2    : row.pk_part_2,
//                        randomInt    : row.randomInt,
//                        randomVarchar: row.randomVarchar
//                ])
//        }
//
//        logger.debug("retrieved from MySQL: " + prettyPrint(toJson(resultSet)))
//
//        replicant.close()
//        activeSchema.close()
//
//        return this
//    }
}