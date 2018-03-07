package booking.replication.it

import com.booking.replication.it.ReplicatorPipeline
import com.booking.replication.it.ReplicatorTest

class PayloadTest extends ReplicatorTest {
    @Override
    boolean does(String env) {
        return env.equals("hbase")
    }
    private tableName = "tpayload"

    ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline) {
        def replicant = pipeline.mysql.getReplicantSql()

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
        def structured = structuralHBase(output)

        def payloadOutput = pipeline.outputContainer.readData(payloadTableName)
        def structuredPayload = structuralHBase(payloadOutput)

        def uuids = structured['8b04d5e3;first']['d:_transaction_uuid']
        def uuidVals = uuids.keySet().sort().collect {
            uuids[it]
        }

        def payloadVals = uuidVals.collect { uuid ->
            def r = [:]
            structuredPayload[uuid].each {k,v ->
                r[k] = k + "|" + v.values()[0]
            }
            r
        }

        return payloadVals.collect {
            it["d:event_id"]+"}{"+it["d:server_role"]+"}{"+it["d:strange_int"]
        }
    }
}
