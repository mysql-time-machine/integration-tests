package booking.replication.it

import com.booking.replication.it.ReplicatorPipeline
import com.booking.replication.it.ReplicatorTest
import groovy.json.JsonSlurper

import static groovy.json.JsonOutput.prettyPrint;
import static groovy.json.JsonOutput.toJson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory

class TransmitInsertsTest extends ReplicatorTest {

    private static final Logger logger = LoggerFactory.getLogger(TransmitInsertsTest.class);

    List<String> getExpected(String env) {
        if (env.equals("kafka")) {
            return [
                    "A|1|665726|PZBAAQSVoSxxFassQEAQ",
                    "B|2|490705|cvjIXQiWLegvLs kXaKH",
                    "C|3|437616|pjFNkiZExAiHkKiJePMp",
                    "D|4|537616|SjFNkiZExAiHkKiJePMp",
                    "E|5|637616|ajFNkiZExAiHkKiJePMp"
            ]
        } else {
            return [
                    "0d61f837;C;3|d:pk_part_1|C",
                    "0d61f837;C;3|d:pk_part_2|3",
                    "0d61f837;C;3|d:randomint|437616",
                    "0d61f837;C;3|d:randomvarchar|pjFNkiZExAiHkKiJePMp",
                    "0d61f837;C;3|d:row_status|I",
                    "3a3ea00c;E;5|d:pk_part_1|E",
                    "3a3ea00c;E;5|d:pk_part_2|5",
                    "3a3ea00c;E;5|d:randomint|637616",
                    "3a3ea00c;E;5|d:randomvarchar|ajFNkiZExAiHkKiJePMp",
                    "3a3ea00c;E;5|d:row_status|I",
                    "7fc56270;A;1|d:pk_part_1|A",
                    "7fc56270;A;1|d:pk_part_2|1",
                    "7fc56270;A;1|d:randomint|665726",
                    "7fc56270;A;1|d:randomvarchar|PZBAAQSVoSxxFassQEAQ",
                    "7fc56270;A;1|d:row_status|I",
                    "9d5ed678;B;2|d:pk_part_1|B",
                    "9d5ed678;B;2|d:pk_part_2|2",
                    "9d5ed678;B;2|d:randomint|490705",
                    "9d5ed678;B;2|d:randomvarchar|cvjIXQiWLegvLs kXaKH",
                    "9d5ed678;B;2|d:row_status|I",
                    "f623e75a;D;4|d:pk_part_1|D",
                    "f623e75a;D;4|d:pk_part_2|4",
                    "f623e75a;D;4|d:randomint|537616",
                    "f623e75a;D;4|d:randomvarchar|SjFNkiZExAiHkKiJePMp",
                    "f623e75a;D;4|d:row_status|I"
            ]
        }
    }

    List<String> getReceived(ReplicatorPipeline pipeline, String env) {
        if (env.equals("kafka")) {
            def allRows = []

            def result = pipeline.outputContainer.readMessagesFromKafkaTopic("replicator_test_kafka",10000);

            def messages = result.getStdout()

            def jsonSlurper = new JsonSlurper()

            messages.eachLine { line ->

                logger.debug("message => " + line.toString())

                def messageEntries = jsonSlurper.parseText(line)

                def inserts =
                        messageEntries['rows'].findAll {
                            it["eventType"] == "INSERT"
                        }

                def rows = inserts.collect {
                    [
                            it["eventColumns"]["pk_part_1"]["value"],
                            it["eventColumns"]["pk_part_2"]["value"],
                            it["eventColumns"]["randomint"]["value"],
                            it["eventColumns"]["randomvarchar"]["value"]
                    ]
                }

                rows.each{ row -> allRows.add(row) }
            }

            def rowsReceived = allRows.collect {
                it.get(0) + "|" +
                        it.get(1) + "|" +
                        it.get(2) + "|" +
                        it.get(3)
            }

            return rowsReceived
        } else {
            def cells = parseHBase(pipeline.outputContainer.readData("sometable"))

            return cells.collect {it[1]+"|"+it[2]+"|"+it[4]}
        }
    }

    ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline) {

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

    @Override
    boolean does(String env) {
        return true
    }
}
