#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
compose=(docker compose -p "${SMOKE_PROJECT:-spark-upgrade-smoke}" -f docker-compose.yml -f dev/compose-smoke.yml)
mode=${1:-smoke}
if [[ "$mode" != smoke && "$mode" != notebooks && "$mode" != all ]]; then
  printf 'Usage: bash dev/smoke-test.sh [smoke|notebooks|all]\n' >&2
  exit 2
fi

"${compose[@]}" exec -T spark-iceberg timeout 180 python -u - <<'PY'
import json
import time
import urllib.request

for url in (
    "http://rest:8181/v1/config",
    "http://minio:9000/minio/health/ready",
    "http://localhost:8888/api/status",
    "http://localhost:8080/json/",
    "http://localhost:18080/api/v1/applications",
):
    deadline = time.monotonic() + 30
    while True:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                assert response.status == 200
                if url.endswith("/json/"):
                    status = json.load(response)
                    assert status["aliveworkers"] >= 1, status
            print("Ready:", url)
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
PY

if [[ "$mode" == smoke || "$mode" == all ]]; then
  "${compose[@]}" exec -T spark-iceberg python -m pip check
  "${compose[@]}" exec -T spark-iceberg aws --version
  "${compose[@]}" exec -T spark-iceberg timeout 600 python -u - <<'PY'
import importlib
import json
import os
import time
import urllib.request
import uuid
from pathlib import Path

import nbformat
import pandas as pd
import pyarrow as pa
import py4j
import pyspark
from nbclient import NotebookClient
from pyiceberg.catalog import load_catalog
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

for module in ("prettytable", "scipy", "matplotlib", "duckdb"):
    importlib.import_module(module)
assert pyspark.__version__ == os.environ["SPARK_VERSION"]
assert tuple(map(int, py4j.__version__.split("."))) >= (0, 10, 9, 9)
jars = Path(os.environ["SPARK_HOME"], "jars")
assert [jar.name for jar in jars.glob("iceberg-spark-runtime-*.jar")] == [
    "iceberg-spark-runtime-4.1_2.13-1.12.0.jar"
]
print("Packaging:", pyspark.__version__, py4j.__version__, py4j.__file__)

application = "upgrade-smoke-" + uuid.uuid4().hex[:12]
spark = (
    SparkSession.builder.appName(application)
    .master("spark://spark-iceberg:7077")
    .config("spark.cores.max", "2")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "512m")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
namespace = "smoke_" + uuid.uuid4().hex[:12]
table = f"demo.{namespace}.rows"
v3_table = f"demo.{namespace}.rows_v3"
view = f"demo.{namespace}.row_view"
load_iceberg_table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable
try:
    assert spark.sparkContext.master == "spark://spark-iceberg:7077"
    assert spark._jvm.scala.util.Properties.versionNumberString().startswith("2.13.")
    assert "1.12.0" in spark._jvm.org.apache.iceberg.IcebergBuild.fullVersion()
    assert spark.sparkContext.parallelize([1, 2], 2).map(lambda value: value + 1).collect() == [2, 3]
    increment = udf(lambda value: value + 1, LongType())
    assert [row.value for row in spark.range(2).select(increment("id").alias("value")).collect()] == [1, 2]
    assert spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() >= 2
    frame = pd.DataFrame({"value": [1, None], "stamp": [pd.Timestamp("2024-01-01"), pd.NaT]})
    actual = spark.createDataFrame(frame, "value double, stamp timestamp").toPandas()
    pd.testing.assert_frame_equal(actual, frame.astype({"value": "float64"}), check_dtype=False)
    print("Standalone Python workers and Arrow passed")

    sql = spark.sql
    sql(f"CREATE NAMESPACE demo.{namespace}")
    sql(f"CREATE TABLE {table} (id BIGINT, value STRING) USING iceberg TBLPROPERTIES ('format-version'='2')")
    assert load_iceberg_table(spark._jsparkSession, table).operations().current().formatVersion() == 2
    sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    snapshot = sql(f"SELECT snapshot_id FROM {table}.snapshots").first().snapshot_id
    sql(f"UPDATE {table} SET value='updated' WHERE id=1")
    sql(f"MERGE INTO {table} target USING (SELECT 2L id, 'merged' value UNION ALL SELECT 3L, 'c') source "
        "ON target.id=source.id WHEN MATCHED THEN UPDATE SET value=source.value "
        "WHEN NOT MATCHED THEN INSERT *")
    sql(f"DELETE FROM {table} WHERE id=3")
    expected = [(1, "updated"), (2, "merged")]
    assert [tuple(row) for row in sql(f"SELECT * FROM {table} ORDER BY id").collect()] == expected
    assert [tuple(row) for row in sql(f"SELECT * FROM {table} VERSION AS OF {snapshot} ORDER BY id").collect()] == [(1, "a"), (2, "b")]
    assert sql(f"SELECT * FROM {table}.snapshots").count() >= 4
    files = sql(f"SELECT file_path FROM {table}.files").collect()
    assert files and all(row.file_path.startswith("s3://warehouse/") for row in files)
    sql(f"ALTER TABLE {table} ADD COLUMN extra STRING")
    assert sql(f"SELECT * FROM {table} WHERE extra IS NOT NULL").count() == 0
    sql(f"ALTER TABLE {table} CREATE BRANCH audit")
    sql(f"INSERT INTO {table}.branch_audit VALUES (4, 'branch', NULL)")
    assert sql(f"SELECT * FROM {table}").count() == 2
    assert sql(f"SELECT * FROM {table}.branch_audit").count() == 3
    sql(f"CALL demo.system.fast_forward('{namespace}.rows', 'main', 'audit')").collect()
    assert sql(f"SELECT * FROM {table}").count() == 3
    sql(f"CREATE VIEW {view} AS SELECT id FROM {table} WHERE id=4")
    assert sql(f"SELECT * FROM {view}").first().id == 4
    sql(f"CREATE OR REPLACE VIEW {view} AS SELECT id FROM {table} WHERE id=2")
    assert sql(f"SELECT * FROM {view}").first().id == 2
    sql(f"DROP VIEW {view}")
    sql(f"ALTER TABLE {table} DROP BRANCH audit")
    print("Iceberg CRUD, schema, snapshots, branches and views passed")

    catalog = load_catalog("default")
    iceberg_table = catalog.load_table(f"{namespace}.rows")
    assert iceberg_table.scan().to_arrow().num_rows == 3
    arrow_schema = iceberg_table.scan().to_arrow().schema
    iceberg_table.append(pa.Table.from_pylist([{"id": 5, "value": "python", "extra": None}], schema=arrow_schema))
    spark.catalog.refreshTable(table)
    assert sql(f"SELECT value FROM {table} WHERE id=5").first().value == "python"
    expected_rows = sql(f"SELECT * FROM {table} ORDER BY id").collect()
    result = sql(f"CALL demo.system.rewrite_data_files(table => '{namespace}.rows', options => map('rewrite-all','true'))").first()
    assert result.rewritten_data_files_count > 0, result
    assert sql(f"SELECT * FROM {table} ORDER BY id").collect() == expected_rows
    assert sql(f"CALL demo.system.rewrite_manifests('{namespace}.rows')").first() is not None
    expired_snapshot = sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id
    sql(f"CALL demo.system.rollback_to_snapshot('{namespace}.rows', {snapshot})").collect()
    assert sql(f"SELECT id, value FROM {table} ORDER BY id").collect() == [(1, "a"), (2, "b")]
    sql(f"CALL demo.system.expire_snapshots(table => '{namespace}.rows', snapshot_ids => ARRAY({expired_snapshot}L))").collect()
    assert sql(f"SELECT * FROM {table}.snapshots WHERE snapshot_id={expired_snapshot}").count() == 0
    assert sql(f"SELECT * FROM {table}").count() == 2
    print("PyIceberg interoperability and maintenance procedures passed")

    sql(f"CREATE TABLE {v3_table} (id BIGINT, value STRING) USING iceberg")
    assert load_iceberg_table(spark._jsparkSession, v3_table).operations().current().formatVersion() == 3
    sql(f"INSERT INTO {v3_table} VALUES (1, 'a'), (2, 'b')")
    assert [tuple(row) for row in sql(f"SELECT * FROM {v3_table} ORDER BY id").collect()] == [(1, "a"), (2, "b")]
    v3_snapshot = sql(f"SELECT snapshot_id FROM {v3_table}.snapshots").first().snapshot_id
    sql(f"UPDATE {v3_table} SET value='updated' WHERE id=1")
    assert [tuple(row) for row in sql(f"SELECT * FROM {v3_table} ORDER BY id").collect()] == [(1, "updated"), (2, "b")]
    sql(f"MERGE INTO {v3_table} target USING (SELECT 2L id, 'merged' value UNION ALL SELECT 3L, 'c') source "
        "ON target.id=source.id WHEN MATCHED THEN UPDATE SET value=source.value "
        "WHEN NOT MATCHED THEN INSERT *")
    assert [tuple(row) for row in sql(f"SELECT * FROM {v3_table} ORDER BY id").collect()] == expected + [(3, "c")]
    sql(f"DELETE FROM {v3_table} WHERE id=3")
    assert [tuple(row) for row in sql(f"SELECT * FROM {v3_table} ORDER BY id").collect()] == expected
    assert [tuple(row) for row in sql(f"SELECT * FROM {v3_table} VERSION AS OF {v3_snapshot} ORDER BY id").collect()] == [(1, "a"), (2, "b")]

    previous_files = {row.file_path for row in sql(f"SELECT file_path FROM {v3_table}.data_files").collect()}
    spark.createDataFrame([(101, "kept"), (102, "deleted"), (103, "kept"), (104, "kept")],
                          "id long, value string").coalesce(1).writeTo(v3_table).append()
    added_files = [row for row in sql(f"SELECT file_path, record_count FROM {v3_table}.data_files").collect()
                   if row.file_path not in previous_files]
    assert len(added_files) == 1 and added_files[0].record_count == 4, added_files
    sql(f"ALTER TABLE {v3_table} SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')")
    sql(f"DELETE FROM {v3_table} WHERE id=102")
    expected_v3 = expected + [(101, "kept"), (103, "kept"), (104, "kept")]
    assert [tuple(row) for row in sql(f"SELECT * FROM {v3_table} ORDER BY id").collect()] == expected_v3
    vectors = sql(f"SELECT referenced_data_file, content_offset, content_size_in_bytes, record_count "
                  f"FROM {v3_table}.delete_files WHERE content=1 AND file_format='PUFFIN'").collect()
    assert len(vectors) == 1, vectors
    assert vectors[0].referenced_data_file == added_files[0].file_path, vectors
    assert vectors[0].content_offset >= 0 and vectors[0].content_size_in_bytes > 0, vectors
    assert vectors[0].record_count == 1, vectors
    result = sql(f"CALL demo.system.rewrite_data_files(table => '{namespace}.rows_v3', options => map('rewrite-all','true'))").first()
    assert result.rewritten_data_files_count > 0, result
    spark.catalog.refreshTable(v3_table)
    assert [tuple(row) for row in sql(f"SELECT * FROM {v3_table} ORDER BY id").collect()] == expected_v3
    print("Default v3 CRUD, time travel, deletion vectors and compaction passed")

    rows_before_upgrade = sql(f"SELECT * FROM {table} ORDER BY id").collect()
    files_before_upgrade = {row.file_path for row in sql(f"SELECT file_path FROM {table}.data_files").collect()}
    assert files_before_upgrade
    sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('format-version'='3')")
    spark.catalog.refreshTable(table)
    assert load_iceberg_table(spark._jsparkSession, table).operations().current().formatVersion() == 3
    assert sql(f"SELECT * FROM {table} ORDER BY id").collect() == rows_before_upgrade
    assert {row.file_path for row in sql(f"SELECT file_path FROM {table}.data_files").collect()} == files_before_upgrade
    sql(f"INSERT INTO {table} VALUES (6, 'upgraded', NULL)")
    assert [tuple(row) for row in sql(f"SELECT * FROM {table} ORDER BY id").collect()] == [
        tuple(row) for row in rows_before_upgrade
    ] + [(6, "upgraded", None)]
    print("Populated v2-to-v3 metadata upgrade and Spark append passed")
finally:
    spark.sql(f"DROP VIEW IF EXISTS {view}")
    spark.sql(f"DROP TABLE IF EXISTS {v3_table} PURGE")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS demo.{namespace}")
    spark.stop()

deadline = time.monotonic() + 60
while True:
    with urllib.request.urlopen("http://localhost:18080/api/v1/applications", timeout=3) as response:
        applications = json.load(response)
    if any(item["name"] == application for item in applications):
        break
    assert time.monotonic() < deadline, applications
    time.sleep(1)
print("Completed application visible in history server")

python_cells = [
    "from pyspark.sql import SparkSession\nspark = SparkSession.builder.master('local[2]').getOrCreate()\nassert spark.version.startswith('4.1.')\nassert spark.range(3).count() == 3",
    "%sql SELECT 1 AS value",
    "%%sql --limit 1\nSELECT 1 AS value",
    "%%sql --var smoke_frame\nSELECT 7 AS value",
    "assert smoke_frame.first().value == 7\nspark.stop()",
]
scala_cells = [
    '//> using script /opt/spark/notebook_kernel/sparkSetup.sc\nimport sparkSetup._\nval spark = createSparkSession()\nval sc = spark.sparkContext',
    'assert(spark.version.startsWith("4.1."))\nassert(spark.range(3).count() == 3)',
    'assert(sc.parallelize(Seq(1, 2), 2).map(value => value + 1).collect().toSeq == Seq(2, 3))',
    "spark.stop()",
]
for kernel, cells in (("python3", python_cells), ("scala", scala_cells)):
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell(source) for source in cells])
    NotebookClient(notebook, kernel_name=kernel, timeout=120, startup_timeout=120).execute(
        env={**os.environ, "SPARK_MASTER": "spark://spark-iceberg:7077"}
    )
    print("Kernel passed:", kernel)
PY

  "${compose[@]}" exec -T spark-iceberg timeout 120 spark-sql -e 'SELECT 1; SHOW NAMESPACES IN demo;'
  "${compose[@]}" exec -T spark-iceberg timeout 120 beeline -u jdbc:hive2://localhost:10000 -n root --force=false -e 'SELECT 1;'
fi

if [[ "$mode" == notebooks || "$mode" == all ]]; then
  "${compose[@]}" exec -T spark-iceberg mkdir -p /tmp/test-results
  for notebook in spark/notebooks/*.ipynb; do
    name=$(basename "$notebook")
    printf 'Running %s\n' "$name"
    "${compose[@]}" exec -T spark-iceberg timeout 1800 papermill \
      "/home/iceberg/notebooks/$name" "/tmp/test-results/$name" \
      --execution-timeout 300 --start-timeout 120
  done
fi

printf 'Passed: %s\n' "$mode"
