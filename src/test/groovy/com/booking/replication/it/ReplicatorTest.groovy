package com.booking.replication.it

import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class ReplicatorTest {
    protected payloadTableName = "__payload__"
    abstract boolean does(String env)
    abstract ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline)
    abstract List<String> getExpected(String env)
    abstract List<String> getReceived(ReplicatorPipeline pipeline, String env)

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorTest.class);

    Collection parseHBaseShellOutput(String result) {
        def parsed = result.split("\n").drop(1).dropRight(3).collect({ (it =~ / (.*) column=(.*), timestamp=(\d+), value=(.*)/)[0] })
        logger.debug("parsed HBase stdout => " + parsed)
        return parsed
    }

    /**
     * This function processes the parsed output of HBase shell
     * and transforms it into a hash of the form:
     *
     *  {
     *      row_id => {
     *        column_name_1 => {
     *          timestamp_1 => value_1,
     *          ...
     *          timestamp_N => value_N,
     *        },
     *        ...
     *        column_name_N => {
     *
     *        }
     *      }
     *  }
     */
    Map structureHBaseOutput(String result) {
        def cells = parseHBaseShellOutput(result)
        def res = [:]
        def r1 = cells.groupBy {it[1]}
        r1.each {k,v ->
            def r_tmp = v.groupBy {it[2]}
            def r = [:]
            r_tmp.each {k1, v1 ->
                r[k1] = v1.groupBy {it[3]}
                r[k1].each {k2,v2 ->
                    r[k1][k2] = r[k1][k2][0][4]
                }
            }
            res[k] = r
        }
        return res
    }

    void createPayloadTable(Sql replicant) {
        def sqlCreate = sprintf('''
        create table if not exists %s (
            event_id char(6) not null,
            server_role varchar(255) not null,
            strange_int int not null,
            primary key (event_id)
        ) ENGINE = BLACKHOLE
        ''', payloadTableName)
        replicant.execute(sqlCreate)
        replicant.commit()
    }
}
