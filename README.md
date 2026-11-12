<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Spark + Iceberg Quickstart Image

This is a docker compose environment to quickly get up and running with a Spark environment and a local REST
catalog, and MinIO as a storage backend.

## Runtime Versions

The local image uses Spark **4.1.4**, Iceberg **1.12.0**, Scala **2.13.17**,
Python **3.14**, and Java **17**. Python dependencies include PyIceberg **0.12.0**
and PyArrow **22.0.0**. PyIceberg has independent release numbering from Iceberg's
Java runtime.

Scala notebooks use Almond **0.15.0**, replacing spylon-kernel, which relies on
private PySpark APIs removed in Spark 4. Their initial setup cell loads the same
Spark distribution and catalog configuration used by Python. Spark 4 ANSI SQL
mode remains enabled.

## Table Format Versions

New tables created through Spark's configured `demo` catalog default to Iceberg
table-format **v3**. This applies to the local image built from this repository;
it does not automatically upgrade existing tables or change other clients'
creation defaults. Explicit v2 creation remains supported:

```sql
CREATE TABLE demo.default.example_v2 (id BIGINT, value STRING)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

Direct PyIceberg-created tables retain their v2 default. The pinned PyIceberg
0.12.0 cannot write v3 manifests, so keep tables that require PyIceberg writes
at v2. Passing the notebooks does not certify every PyIceberg v3 read path or
every v3 feature, such as new types, defaults, or row lineage.

After validating every reader and writer, an existing table can be upgraded
explicitly:

```sql
ALTER TABLE demo.default.example_v2
SET TBLPROPERTIES ('format-version' = '3');
```

This changes metadata without bulk-rewriting existing data files. The upgrade is
one-way under supported APIs: snapshot rollback does not downgrade the format,
and the format applies to the whole table, including its branches. Do not use
older clients or the separate Flink example against upgraded tables without
their own compatibility validation.

**note**: If you don't have docker installed, you can head over to the [Get Docker](https://docs.docker.com/get-docker/)
page for installation instructions.

## Usage
Start up the notebook server by running the following.
```
docker-compose up
```

The notebook server will then be available at http://localhost:8888

While the notebook server is running, you can use any of the following commands if you prefer to use spark-shell, spark-sql, or pyspark.
```
docker exec -it spark-iceberg spark-shell
```
```
docker exec -it spark-iceberg spark-sql
```
```
docker exec -it spark-iceberg pyspark
```

To stop everything, just run `docker-compose down`.

## Troubleshooting & Maintenance

### Refreshing Docker Image

The prebuilt spark image is uploaded to Dockerhub. Out of convenience, the image tag defaults to `latest`.

If you have an older version of the image, you might need to remove it to upgrade.
```bash
docker image rm tabulario/spark-iceberg && docker-compose pull
```

### Building the Docker Image locally

If you want to make changes to the local files, and test them out, you can build the image locally and use that instead:

```bash
docker compose build spark-iceberg
docker compose up -d --no-build
```

### Use `Dockerfile` In This Repo

To directly use the Dockerfile in this repo (as opposed to pulling the pre-build `tabulario/spark-iceberg` image), you can use `docker-compose build`.

### Smoke Tests and Notebooks

Use Docker Compose 2.24.4 or newer for the test override's `!reset` tags. The test
stack uses a separate network, no fixed container names or published host ports,
and no host notebook or warehouse mounts. It can run alongside the normal stack.

From the repository root:

```bash
export COMPOSE_PROJECT_NAME=spark-upgrade-smoke
export COMPOSE_FILE=docker-compose.yml:dev/compose-smoke.yml
docker compose build spark-iceberg
docker compose up -d --no-build
bash dev/smoke-test.sh smoke
bash dev/smoke-test.sh notebooks
```

The smoke tests check Python workers on the standalone Spark cluster, Arrow
round-trips, REST/MinIO reads and writes, SQL updates and merges, snapshots,
maintenance procedures, branches, views, PyIceberg interoperability, Python and
Scala kernels, SQL notebook magics, the history server, and a real JDBC query.
They also verify the v3 creation default, explicit v2 compatibility, v3 CRUD and
time travel, a partial-row delete producing a Puffin deletion vector, compaction
correctness, and a populated v2-to-v3 upgrade retaining its existing data files.
PyIceberg writes are tested on v2 before that fixture's upgrade.
The notebook command executes all ten examples sequentially with timeouts and
writes results inside the container under `/tmp/test-results`. Use `all` to run
both commands together. Copy results before stopping the test stack:

```bash
docker compose cp spark-iceberg:/tmp/test-results ./test-results
docker compose down --volumes
```

Allow several minutes and roughly 8 GiB of Docker memory for the multi-JVM test
stack. An initial build also downloads Spark, dependencies, and the sample data.
CI runs the smoke suite on AMD64 and ARM64, and the full notebook suite on AMD64.

Local validation of the v3 default passed the full smoke suite and all ten
unchanged notebooks on native ARM64 with Python 3.14. Earlier AMD64 emulation of
the runtime passed the non-notebook checks but stalled during Python kernel
platform detection in debugger initialization. Native AMD64 CI remains a required
release gate; an image build alone does not certify that runtime.

**Data safety:** the `mc` service deletes and recreates the `warehouse` bucket
when initialized. Both the normal stack and the examples are disposable demos,
not an in-place migration procedure for existing data. Do not configure them to
use a production bucket. An older image is not a rollback strategy for table
metadata or table-format upgrades.

The REST fixture and Chainguard MinIO images are pinned by digest. MinIO remains
the S3 backend; its original Docker Hub images are no longer publicly available.
The separate Flink example consumes the shared Spark image, but its Flink runtime
and connector versions are not upgraded or certified by these tests.

### Deploying Changes

To deploy changes to the hosted docker image `tabulario/spark-iceberg`, run the following. (Requires access to the tabulario docker hub account)

```sh
cd spark
docker buildx build -t tabulario/spark-iceberg --platform=linux/amd64,linux/arm64 . --push
```

---

For more information on getting started with using Iceberg, checkout
the [Quickstart](https://iceberg.apache.org/spark-quickstart/) guide in the official docs.

The repository for the docker image is [located on dockerhub](https://hub.docker.com/r/tabulario/spark-iceberg).
