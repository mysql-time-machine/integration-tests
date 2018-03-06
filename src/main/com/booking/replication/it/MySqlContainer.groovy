package com.booking.replication.it

import com.github.dockerjava.api.command.InspectContainerResponse
import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network

public class MySqlContainer extends GenericContainer<KafkaContainer> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaContainer.class);

    public MySqlContainer(Network network, String imageTag) {

        super(imageTag);

        withNetwork(network);
        withNetworkAliases("mysql");
        withClasspathResourceMapping(
                "my.cnf",
                "/etc/mysql/conf.d/my.cnf",
                BindMode.READ_ONLY
        );
        withClasspathResourceMapping(
                "mysql_init_dbs.sh",
                "/docker-entrypoint-initdb.d/mysql_init_dbs.sh",
                BindMode.READ_ONLY
        );
        withEnv("MYSQL_ROOT_PASSWORD", "mysqlPass");
        withExposedPorts(3306);

    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        logger.info("mysql container started at { ip: " + getContainerIpAddress() + ", port: " + getMappedPort(3306));
    }

    public Sql getReplicantSql() {

        def urlReplicant = 'jdbc:mysql://' + getContainerIpAddress() + ":" + getMappedPort(3306) + '/test'

        logger.debug("jdbc url: " + urlReplicant);

        def dbReplicant = [
                url     : urlReplicant,
                user    : 'root',
                password: 'mysqlPass',
                driver  : 'com.mysql.jdbc.Driver'
        ]

        def replicant = Sql.newInstance(
                dbReplicant.url,
                dbReplicant.user,
                dbReplicant.password,
                dbReplicant.driver
        );

        replicant.connection.autoCommit = false

        return replicant;
    }

    public Sql getActiveSchemaSql() {

        def urlActiveSchema = 'jdbc:mysql://' + getContainerIpAddress() + ":" + getMappedPort(3306)+ '/test_active_schema'

        logger.debug("jdbc url: " + urlActiveSchema)

        def dbActiveSchema = [
                url     : urlActiveSchema,
                user    : 'root',
                password: 'mysqlPass',
                driver  : 'com.mysql.jdbc.Driver'
        ]

        def activeSchema = Sql.newInstance(
                dbActiveSchema.url,
                dbActiveSchema.user,
                dbActiveSchema.password,
                dbActiveSchema.driver
        )

        activeSchema.connection.autoCommit = false

        return activeSchema;
    }
}
