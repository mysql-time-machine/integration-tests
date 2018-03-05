package booking.replication.it

import com.booking.replication.it.KafkaPipeline;

import static groovy.json.JsonOutput.prettyPrint;
import static groovy.json.JsonOutput.toJson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory

/**
 * Created by bdevetak on 3/5/18.
 */
public class TransmitInsertsTest {

    private static final Logger logger = LoggerFactory.getLogger(TransmitInsertsTest.class);

    def KafkaPipeline doMySqlOperations(KafkaPipeline pipeline) {

        def replicant = pipeline.mysql.getReplicantSql()

        // CREATE
        def sqlCreate = """
        CREATE TABLE IF NOT EXISTS
            sometable (
            pk_part_1         varchar(5) NOT NULL DEFAULT '',
            pk_part_2         int(11)    NOT NULL DEFAULT 0,
            randomInt         int(11)             DEFAULT NULL,
            randomVarchar     varchar(32)         DEFAULT NULL,
            PRIMARY KEY       (pk_part_1,pk_part_2),
            KEY randomVarchar (randomVarchar),
            KEY randomInt     (randomInt)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """

        replicant.execute(sqlCreate);
        replicant.commit();

        // INSERT
        def testRows = [
                            ['A', '1', '665726', 'PZBAAQSVoSxxFassQEAQ'],
                            ['B', '2', '490705', 'cvjIXQiWLegvLs kXaKH'],
                            ['C', '3', '437616', 'pjFNkiZExAiHkKiJePMp'],
                            ['D', '4', '537616', 'SjFNkiZExAiHkKiJePMp'],
                            ['E', '5', '637616', 'ajFNkiZExAiHkKiJePMp']
                    ]

        testRows.each {
            row ->
            try {
                def sqlString = """
                INSERT INTO
                sometable (
                        pk_part_1,
                        pk_part_2,
                        randomInt,
                        randomVarchar
                )
                values (
                        ${row[0]},
                ${row[1]},
                ${row[2]},
                ${row[3]}
                                        )
                """

                replicant.execute(sqlString)
                replicant.commit()
            } catch (Exception ex) {
                replicant.rollback()
            }
        }

        // SELECT CHECK
        def resultSet = []
        replicant.eachRow('select * from sometable') {
            row ->
            resultSet.add([
                pk_part_1    : row.pk_part_1,
                pk_part_2    : row.pk_part_2,
                randomInt    : row.randomInt,
                randomVarchar: row.randomVarchar
            ])
    }

        logger.debug("retrieved from MySQL: " + prettyPrint(toJson(resultSet)))

        replicant.close()

        return pipeline

    }
}
