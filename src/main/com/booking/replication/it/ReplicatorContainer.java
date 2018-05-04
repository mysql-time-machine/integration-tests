package com.booking.replication.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.RemoteDockerImage;

import java.io.IOException;

/**
 * Created by bdevetak on 3/5/18.
 */
public class ReplicatorContainer extends GenericContainer {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorContainer.class);

    public ReplicatorContainer(String imageTag, Network network) {

        super(new RemoteDockerImage(imageTag));

        withNetwork(network);
        withNetworkAliases("replicator");
        withClasspathResourceMapping(
            "replicator-conf.yaml",
            "/replicator/replicator-conf.yaml",
            BindMode.READ_ONLY
        );
    }

    // TODO: manage replicator version tests properly with docker tags
    public ReplicatorPipeline startReplicationFromFirstBinlogFile_V0145(ReplicatorPipeline pipeline, String applier) {

        Runnable task = () -> {

            ExecResult result = null;
            try {
                result = this.execInContainer(
                        "java",
                        "-jar", "/replicator/mysql-replicator-0.14.5.jar",
                        "--applier", applier,
                        "--schema", "test",
                        "--binlog-filename", "binlog.000001",
                        "--config-path", "/replicator/replicator-conf.yaml"
                );

                logger.info(result.getStderr());
                logger.info(result.getStdout());

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        };

        Thread thread = new Thread(task);
        thread.start();

        return pipeline;
    }

    public ReplicatorPipeline startReplicationFromPGTID_V0145(ReplicatorPipeline pipeline, String applier) {

        Runnable task = () -> {

            ExecResult result = null;
            try {
                result = this.execInContainer(
                        "java",
                        "-jar", "/replicator/mysql-replicator-0.14.5.jar",
                        "--applier", applier,
                        "--schema", "test",
                        "--config-path", "/replicator/replicator-conf.yaml"
                );

                logger.info(result.getStderr());
                logger.info(result.getStdout());

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        };

        Thread thread = new Thread(task);
        thread.start();

        return pipeline;
    }

    public ReplicatorPipeline startReplicationFromFirstBinlogFile_V015(
            ReplicatorPipeline pipeline,
            String applier,
            String parser) {

        Runnable task = () -> {

            ExecResult result = null;
            try {
                result = this.execInContainer(
                        "java",
                        "-jar", "/replicator/mysql-replicator-0.15.0-alpha-SNAPSHOT.jar",
                        "--parser", parser,
                        "--applier", applier,
                        "--schema", "test",
                        "--binlog-filename", "binlog.000001",
                        "--config-path", "/replicator/replicator-conf.yaml"
                );

                logger.info(result.getStderr());
                logger.info(result.getStdout());

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        };

        Thread thread = new Thread(task);
        thread.start();

        return pipeline;
    }
}
