package com.booking.replication.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;

public class HBaseContainer extends FixedHostPortGenericContainer<HBaseContainer> {
    private static final Logger logger = LoggerFactory.getLogger(HBaseContainer.class);
    public HBaseContainer(Network network) {

        super("harisekhon/hbase-dev:latest");
        withNetwork(network);
        withNetworkAliases("hbase");
    }

    public String readData(String tableName) throws IOException, InterruptedException {

        ExecResult res = shellCommand("scan 'test:"+tableName+"', {VERSIONS => 20}");

        return res.getStdout();

    }

    @Override
    public void start() {
        super.start();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        createNamespace("test");
        createNamespace("schema_history");
    }

    private void createNamespace(String namespace) {
        logger.info("Creating namespace "+namespace);
        shellCommand("create_namespace '"+namespace+"'");
    }

    private ExecResult shellCommand(String cmd) {
        ExecResult res = null;
        try {
            res = this.execInContainer(
                    "sh",
                    "-c",
                    "echo -e \""+cmd+"\" | /hbase/bin/hbase shell -n"
            );
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }
}
