1. Add readiness check for the replicator container
2. Remove test specific code from the pipeline
3. Add automatic replicator image building (currently needs to be
   done manually as described in https://github.com/mysql-time-machine/replicator/blob/master/README.md
4. Parametrize test-run with image tags (for MySQL) and replicator release tags.
5. Add HBase pipeline
6. Add MySQL replication chain to the pipeline (one MySQL master, multiple MySQL slaves)
7. Add failover options to the pipeline (kill_and_restart replicator, crash_mysql_slave)
8. Add following tests:
    - replicator leadership election (start multiple replicators + crash_leader)
    - pGTID failover during MySQL crash
    - microseconds consistency during pGTID failover
    - long transactions timestamps
