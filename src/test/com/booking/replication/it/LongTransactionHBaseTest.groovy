package booking.replication.it

import com.booking.replication.it.ReplicatorPipeline
import com.booking.replication.it.ReplicatorTest
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang.reflect.InvocationTargetException

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

        def replicant = pipeline.mysql.getReplicantSql(
            false // <- autoCommit
        )

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
                "insert into %s %s values ('user',42,1,'xx')", tableName, columns
        ))
        replicant.commit()

        // UPDATE 1
        def where = " where pk_part_1 = 'user' and pk_part_2 = 42"
        replicant.execute(sprintf(
                "update %s set randomInt = 2, randomVarchar = 'yy' %s", tableName, where
        ))

        // Sleep to simulate long transaction
        Thread.sleep(5000);

        // UPDATE 2
        replicant.execute(sprintf(
                "update %s set randomInt = 3, randomVarchar = 'zz' %s", tableName, where
        ))

        replicant.commit()
        replicant.close()

        return pipeline
    }

    List<String> getExpected(String env) {
        // Even though there is a sleep of 5 seconds in between two updates,
        // both updates are part of the same transaction. So, they should
        // be 1 and 2 microseconds shifted from the commit timestamp of that
        // transaction.
        return [
                "true", // timestamps of first update is close to commit time of updates transaction
                "1"     // expected diff between versions in microseconds
        ]
    }

    List<String> getReceived(ReplicatorPipeline pipeline, String env) {

        def output = pipeline.outputContainer.readData(tableName)
        def cells = parseHBaseShellOutput(output)
        def structured = structureHBaseOutput(output)


        def ri_cells = structured['ee11cbb1;user;42']['d:randomint']


        def timestamps = ri_cells.keySet().sort();

        // ==============================================================
        // Original time line:
        // t0 (insert & commit)
        // t1 ~ t0 (update 1)
        // sleep 5s
        // t2 ~ t1 + 5s (update 2 & commit)
        //
        // microsecond override:
        // t2 = t1 + 1micros ~ t0 + 5s

        // this is the diff in timestamps between insert and first update after
        // microsecond logic is applied.
        def diff_01 = (Long.parseLong(timestamps[1]) -  Long.parseLong(timestamps[0]))

        // there is a 5s sleep between two updates and no sleep between insert and
        // fist update, so assumption is that the commit time after the second
        // update will be roughly 5s after the insert commit. Roughly is estimated
        // to 5ms precision when running tests on the same machine as docker containers.
        // Since replicator pins the time of all updates in the transaction to be around
        // the commit time of the transaction, this means we expect the first update
        // to be close to commit time of updates transaction, which means that
        // the timestamp of first update should be be roughly 5s after the insert timestamp.
        def estimated_reasonable_lag = 5000 // micros
        def drift_due_to_lag = Math.abs(5000000 - diff_01);
        def commit_time_in_boundaries = (drift_due_to_lag < estimated_reasonable_lag)

        // this is the diff between timestamps of two updates, after microsecond logic
        // is applied.
        def diff_12 = (Long.parseLong(timestamps[2]) -  Long.parseLong(timestamps[1]))

        return [
                String.valueOf(commit_time_in_boundaries),
                diff_12.toString()
        ];
    }
}
