package booking.replication.it

import com.booking.replication.it.ReplicatorPipeline
import com.booking.replication.it.ReplicatorTest
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Inserts multiple rows in a long transaction and verifies
 * that the timestamps of all rows are close to the time stamp
 * of the transaction commit event.
 *
 * Problem description:
 *
 *  (a) Long transaction
 *
 *      BEGIN
 *      insert row1, column1, value1
 *      sleep 30s
 *      insert row1, column1, value2
 *      COMMIT
 *
 *  (b) Ordinary transaction
 *
 *      BEGIN
 *      insert row1, column1, value1
 *      update row1, column1, value2
 *      COMMIT
 *
 *  If we would not apply any changes to the timestamps of the above
 *  written values and just write them to HBase as we read them from
 *  the binlog, we would have the following problem:
 *
 *    - For long transaction the timestamps of the row1 and row2 in HBase
 *      would be 30 seconds apart which would give a fake impression that
 *      these events are separated in time 30s. However, they only become
 *      valid at commit time, so they become effective at the same time,
 *      not 30 seconds apart.
 *
 *  In order to solve that problem we override the row timestamp with the
 *  commit timestamp.
 *
 *  However, this creates a new problem for non-long transactions (b).
 *
 *    - Since mysql timestamp precision is 1 second, the value1 and value2
 *      will have the same timestamp and since they belong to the same
 *      row and column, the value2 will overwrite value1, so we will
 *      lose the information on history of operations in HBase.
 *
 *  To solve that problem, we still override the value timestamps
 *  with the commit timestamp, but in addition add a small shift in
 *  microseconds:
 *
 *      timestamp_row1_column1_value1 = commit_timestamp - 49 microseconds
 *      timestamp_row1_column1_value2 = commit_timestamp - 48 microseconds
 *
 *  This way, the values will have the same commit timestamp when
 *  rounded to the precision of one second and on the other hand
 *  we will still have all rows preserved in HBase.
 *
 *  To summarize: without the microsecond addition to the commit_timestamp,
 *  value2 would override the value1 in transaction that is shorter than 1
 *  second since MySQL timestamp precision is 1 seconds. On the other
 *  hand, without override to the commit_timestamp, in the long transaction
 *  the values would be spread in time to far away from the actual commit
 *  time that matters.
 */
class LongTransactionHBaseTest extends ReplicatorTest {

    private static final Logger logger = LoggerFactory.getLogger(LongTransactionHBaseTest.class);

    @Override
    boolean does(String env) {
        return env.equals("hbase")
    }

    private tableName = "micros_long_transaction_test"

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

        // INSERT
        def columns = "(pk_part_1,pk_part_2,randomInt,randomVarchar)"
        replicant.execute(sprintf(
                "insert into %s %s values ('user',42,1,'zz')", tableName, columns
        ))

        // UPDATE 1
        def where = " where pk_part_1 = 'user' and pk_part_2 = 42"
        replicant.execute(sprintf(
                "update %s set randomInt = 2, randomVarchar = 'yy' %s", tableName, where
        ))

        Thread.sleep(5000);

        // UPDATE 2
        replicant.execute(sprintf(
                "update %s set randomInt = 3, randomVarchar = 'xx' %s", tableName, where
        ))

        replicant.commit()
        replicant.close()

        return pipeline
    }

    List<String> getExpected(String env) {
        return ["1|2|3"]
    }

    List<String> getReceived(ReplicatorPipeline pipeline, String env) {

        def output = pipeline.outputContainer.readData(tableName)
        def cells = parseHBaseShellOutput(output)
        def structured = structureHBaseOutput(output)

        def ri = structured['ee11cbb1;user;42']['d:randomint']

        def ri_vals_with_timestamps = ri.keySet().sort().collect { k,v ->
            k + "." + v
        }

        logger.warn(ri_vals_with_timestamps)

        return ri_vals_with_timestamps
    }

}
