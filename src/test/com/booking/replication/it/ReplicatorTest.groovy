package com.booking.replication.it

import groovy.sql.Sql

abstract class ReplicatorTest {
    protected payloadTableName = "__payload__"
    abstract boolean does(String env)
    abstract ReplicatorPipeline doMySqlOperations(ReplicatorPipeline pipeline)
    abstract List<String> getExpected(String env)
    abstract List<String> getReceived(ReplicatorPipeline pipeline, String env)

    Collection parseHBase(String result) {
        return result.split("\n").drop(1).dropRight(3).collect({ (it =~ / (.*) column=(.*), timestamp=(\d+), value=(.*)/)[0] })
    }

    Map structuralHBase(String result) {
        def cells = parseHBase(result)
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
        ) engine = blackhole
        ''', payloadTableName)
        replicant.execute(sqlCreate)
        replicant.commit()
    }
}