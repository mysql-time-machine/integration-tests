Integrations tests and related tooling for the mysql-time-machine pipelines.

Steps to run

```
1. Build replicator-runner image:

    git clone https://github.com/mysql-time-machine/docker.git
    cd docker/images/003_debug_image
    ./container_build.sh 
    ...
    Successfully tagged replicator-runner:latest  

2. Run tests:

    git clone https://github.com/mysql-time-machine/integration-tests.git
    cd integration-tests
    mvn test
```

