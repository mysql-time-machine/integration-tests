package com.booking.replication.it
/**
 * This test verifies that we get the payloads injected into transactions and
 * that we get them in the same order (when sorted by timestamp) in which
 * transactions were made
 * */
class PayloadTest extends ReplicatorTest {

    @Override
    boolean does(String env) {
        return env.equals("hbase")
    }
    private tableName = "tpayload"

    ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline) {
        def replicant = pipeline.mysql.getReplicantSql(
                false // <- autoCommit
        )

        createPayloadTable(replicant)

        // CREATE
        def sqlCreate = sprintf("""
        CREATE TABLE IF NOT EXISTS
            %s (
            pk          varchar(5) NOT NULL DEFAULT '',
            val         int(11)             DEFAULT NULL,
            PRIMARY KEY       (pk)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """, tableName)

        replicant.execute(sqlCreate)
        replicant.commit()
        def columns = "(pk,val)"

        replicant.execute(sprintf(
                "insert into %s %s values ('first',1)", tableName, columns
        ))
        def where = " where pk = 'first'"
        replicant.execute(sprintf(
                "update %s set val = 2 %s", tableName, where
        ))
        replicant.execute(sprintf("insert into %s (event_id,server_role,strange_int) values ('aabbcc','admin',7)", payloadTableName))
        replicant.commit()

        replicant.execute(sprintf(
                "update %s set val = 12 %s", tableName, where
        ))
        replicant.execute(sprintf("insert into %s (event_id,server_role,strange_int) values ('aabbdd','client',17)", payloadTableName))
        replicant.commit()

        replicant.close()

        return pipeline
    }

    List<String> getExpected(String env) {
        return ["d:event_id|aabbcc}{d:server_role|admin}{d:strange_int|7",
                "d:event_id|aabbdd}{d:server_role|client}{d:strange_int|17"]
    }

    List<String> getReceived(ReplicatorPipeline pipeline, String env) {
        def output = pipeline.outputContainer.readData(tableName)
        def structured = structureHBaseOutput(output)

        def payloadOutput = pipeline.outputContainer.readData(payloadTableName)
        def structuredPayload = structureHBaseOutput(payloadOutput)

        def uuids = structured['8b04d5e3;first']['d:_transaction_uuid']
        def transactionUUIDs = uuids.keySet().sort().collect { // <- get transaction UUIDs sorted by timestamp
            uuids[it]
        }

        def payloadVals = transactionUUIDs.collect { transactionUUID ->
            def r = [:]
            structuredPayload[transactionUUID].each { columnName,v -> // (k,v) is (columnName, {timestamp => value})
                r[columnName] = columnName + "|" + v.values()[0] // "column_name|column_value"
            }
            r
        }

        return payloadVals.collect {
            it["d:event_id"]+"}{"+it["d:server_role"]+"}{"+it["d:strange_int"]
        }
    }
}
