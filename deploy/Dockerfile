# One image for every Strata process. Build the distributions first:
#   ./gradlew :controller:installDist :storage-node:installDist :push-job:installDist :bench:installDist
#   docker build -f deploy/Dockerfile -t strata:0.1 .
FROM eclipse-temurin:21-jre
COPY controller/build/install/controller /opt/strata/controller
COPY storage-node/build/install/storage-node /opt/strata/storage-node
COPY push-job/build/install/push-job /opt/strata/push-job
COPY bench/build/install/bench /opt/strata/bench
ENV PATH="/opt/strata/controller/bin:/opt/strata/storage-node/bin:/opt/strata/push-job/bin:/opt/strata/bench/bin:${PATH}"
