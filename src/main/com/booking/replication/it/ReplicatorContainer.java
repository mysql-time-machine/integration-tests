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

    public KafkaPipeline startReplication(KafkaPipeline pipeline) {

        Runnable task = () -> {

            ExecResult result = null;
            try {
                result = this.execInContainer(
                        "java",
                        "-jar", "/replicator/mysql-replicator.jar",
                        "--applier", "kafka",
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
