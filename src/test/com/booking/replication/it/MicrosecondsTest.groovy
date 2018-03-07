package booking.replication.it

import com.booking.replication.it.ReplicatorPipeline
import com.booking.replication.it.ReplicatorTest

class MicrosecondsTest extends ReplicatorTest {
    @Override
    boolean does(String env) {
        return env.equals("hbase")
    }
    private tableName = "tmicros"

    ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline) {
        def replicant = pipeline.mysql.getReplicantSql()

        // CREATE
        def sqlCreate = sprintf("""
        CREATE TABLE IF NOT EXISTS
            %s (
            pk_part_1         varchar(5) NOT NULL DEFAULT '',
            pk_part_2         int(11)    NOT NULL DEFAULT 0,
            randomInt         int(11)             DEFAULT NULL,
            randomVarchar     varchar(32)         DEFAULT NULL,
            PRIMARY KEY       (pk_part_1,pk_part_2),
            KEY randomVarchar (randomVarchar),
            KEY randomInt     (randomInt)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """, tableName)

        replicant.execute(sqlCreate)
        replicant.commit()
        def columns = "(pk_part_1,pk_part_2,randomInt,randomVarchar)"

        replicant.execute(sprintf(
                "insert into %s %s values ('user',42,1,'zz')", tableName, columns
        ))
        def where = " where pk_part_1 = 'user' and pk_part_2 = 42"
        replicant.execute(sprintf(
                "update %s set randomInt = 2, randomVarchar = 'yy' %s", tableName, where
        ))
        replicant.execute(sprintf(
                "update %s set randomInt = 3, randomVarchar = 'xx' %s", tableName, where
        ))
        replicant.commit()
        replicant.close()

        return pipeline
    }

    List<String> getExpected(String env) {
        return ["1|2|3","zz|yy|xx"]
    }

    List<String> getReceived(ReplicatorPipeline pipeline, String env) {
        def output = pipeline.outputContainer.readData(tableName)
        def cells = parseHBase(output)
        def structured = structuralHBase(output)

        def ri = structured['ee11cbb1;user;42']['d:randomint']
        def ri_vals = ri.keySet().sort().collect {
            ri[it]
        }

        def rc = structured['ee11cbb1;user;42']['d:randomvarchar']
        def rc_vals = ri.keySet().sort().collect {
            rc[it]
        }

        return [
                ri_vals.join("|"),
                rc_vals.join("|")
        ]
    }
}