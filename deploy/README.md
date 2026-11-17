# Running Strata with Docker Compose

Kafka (KRaft, single broker), a controller and three storage nodes, each in its own container. The SST "object store" is a shared Docker volume. Verified end to end on 2026-11-17 with Docker 29.

```sh
# from the repository root
./gradlew :controller:installDist :storage-node:installDist :push-job:installDist :bench:installDist
cd deploy
docker compose up -d --build
```

## Walkthrough: the "Ravi" scenario on real Kafka
```sh
A="../controller/build/install/controller/bin/strata-admin --controller=localhost:7000"
T="docker compose run --rm -q -v $PWD/in:/in tools"          # a container on the cluster network
mkdir -p in && printf 'ravi\t111\nasha\t555\n' > in/v1.tsv && printf 'ravi\t111\nasha\t556\n' > in/v2.tsv

$A create-store --name=demo --partitions=4 --rf=2 --skew-ms=1000
$T push-job --objects=/objects --store=demo --partitions=4 --version=1 --input=/in/v1.tsv --work=/tmp \
   --watermark=$(( $(date +%s%3N) - 60000 ))
$A start-push --store=demo --version=1
$T bench get --controller=controller:7000 --store=demo --key=ravi             # 111

CUTOFF=$(date +%s%3N); sleep 2                                                 # v2's snapshot cutoff
$T bench produce --kafka=kafka:9092 --store=demo --partitions=4 --key=ravi --value=222
$T bench get --controller=controller:7000 --store=demo --key=ravi             # 222

$T push-job --objects=/objects --store=demo --partitions=4 --version=2 --input=/in/v2.tsv --work=/tmp \
   --watermark=$CUTOFF                                                         # still says ravi=111
$A start-push --store=demo --version=2
$T bench get --controller=controller:7000 --store=demo --key=ravi             # 222: replayed from Kafka

docker kill deploy-node1-1                                                     # SIGKILL a node
$T bench get --controller=controller:7000 --store=demo --key=ravi             # still 222 (failover)
docker start deploy-node1-1                                                    # rebuilds from SSTs + Kafka
$A view                                                                        # node1 ready again (~2 s)
```

Observed on the dev machine: every step returned the value shown, and node1 was serving its partitions again about 2 s after restart.

## Notes
- Storage nodes advertise `nodeN:7101`, which resolves only inside the Compose network, so run clients in the `tools` container (as above) or add host networking.
- `KAFKA_LOG_RETENTION_HOURS` must exceed the snapshot cutoff-to-swap delay plus clock skew (ADR 0004), or replay can't find the updates it needs.
- The image is about 1.1 GB because it bundles four application distributions, each with its own copy of the RocksDB native library. A single combined distribution would shrink it.
- Tear down with `docker compose down -v`, and remove the image with `docker rmi strata:0.1`.
