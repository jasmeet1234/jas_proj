import argparse
import json
import os
import time
import urllib.request

import duckdb

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


S3_BASE_URL = os.environ.get(
    "REDSET_S3_BASE",
    "https://s3.amazonaws.com/redshift-downloads/redset",
)


def download_parquet(source: str, dest: str) -> str:
    if os.path.exists(dest):
        return dest
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    url = f"{S3_BASE_URL}/{source}/{os.path.basename(dest)}"
    with urllib.request.urlopen(url) as response, open(dest, "wb") as handle:
        handle.write(response.read())
    return dest


def build_clean_view(conn: duckdb.DuckDBPyConnection, parquet_path: str, deployment_type: str):
    safe_path = parquet_path.replace("'", "''")
    conn.execute(f"CREATE OR REPLACE VIEW events_raw AS SELECT * FROM read_parquet('{safe_path}')")
    conn.execute(
        """
        CREATE OR REPLACE VIEW events_clean AS
        SELECT
            CAST(instance_id AS BIGINT) AS instance_id,
            CAST(cluster_size AS BIGINT) AS cluster_size,
            CAST(user_id AS BIGINT) AS user_id,
            CAST(database_id AS BIGINT) AS database_id,
            CAST(query_id AS BIGINT) AS query_id,
            CAST(arrival_timestamp AS TIMESTAMP) AS arrival_timestamp,
            GREATEST(CAST(compile_duration_ms AS DOUBLE), 0) AS compile_duration_ms,
            GREATEST(CAST(queue_duration_ms AS DOUBLE), 0) AS queue_duration_ms,
            GREATEST(CAST(execution_duration_ms AS DOUBLE), 0) AS execution_duration_ms,
            CAST(feature_fingerprint AS VARCHAR) AS feature_fingerprint,
            CAST(was_aborted AS BOOLEAN) AS was_aborted,
            CAST(was_cached AS BOOLEAN) AS was_cached,
            CASE
                WHEN CAST(was_cached AS BOOLEAN) IS TRUE
                    THEN CAST(cache_source_query_id AS BIGINT)
                ELSE NULL
            END AS cache_source_query_id,
            CASE
                WHEN TRIM(LOWER(CAST(query_type AS VARCHAR))) = '' THEN 'other'
                ELSE TRIM(LOWER(CAST(query_type AS VARCHAR)))
            END AS query_type,
            GREATEST(CAST(num_permanent_tables_accessed AS BIGINT), 0) AS num_permanent_tables_accessed,
            GREATEST(CAST(num_external_tables_accessed AS BIGINT), 0) AS num_external_tables_accessed,
            GREATEST(CAST(num_system_tables_accessed AS BIGINT), 0) AS num_system_tables_accessed,
            CAST(read_table_ids AS VARCHAR) AS read_table_ids,
            CAST(write_table_ids AS VARCHAR) AS write_table_ids,
            GREATEST(CAST(mbytes_scanned AS DOUBLE), 0) AS mbytes_scanned,
            GREATEST(CAST(mbytes_spilled AS DOUBLE), 0) AS mbytes_spilled,
            GREATEST(CAST(num_joins AS BIGINT), 0) AS num_joins,
            GREATEST(CAST(num_scans AS BIGINT), 0) AS num_scans,
            GREATEST(CAST(num_aggregations AS BIGINT), 0) AS num_aggregations,
            ? AS deployment_type
        FROM events_raw
        """,
        [deployment_type],
    )


def iter_rows(conn: duckdb.DuckDBPyConnection):
    cursor = conn.execute(
        """
        SELECT *
        FROM events_clean
        ORDER BY arrival_timestamp ASC
        """
    )
    cols = [d[0] for d in cursor.description]
    while True:
        batch = cursor.fetchmany(1000)
        if not batch:
            break
        for row in batch:
            yield dict(zip(cols, row))


def replay_events(rows, producer, topic: str, speed_factor: float):
    previous_ts = None
    start_wall = time.time()
    for row in rows:
        event_ts = row["arrival_timestamp"]
        if previous_ts is None:
            previous_ts = event_ts
            start_wall = time.time()
        else:
            delta = (event_ts - previous_ts).total_seconds()
            if delta > 0:
                time.sleep(delta / speed_factor)
            previous_ts = event_ts

        row["arrival_timestamp"] = event_ts.isoformat()
        payload = json.dumps(row, default=str).encode("utf-8")
        producer.send(topic, payload)
    producer.flush()


def main():
    parser = argparse.ArgumentParser(description="Stream Redset parquet data to Kafka.")
    parser.add_argument("--source", choices=["serverless", "provisioned"], required=True)
    parser.add_argument(
        "--dataset",
        choices=["full", "sample_0.01", "sample_0.001"],
        default="sample_0.001",
    )
    parser.add_argument("--output", default="./data/redset")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="redset-events")
    parser.add_argument("--speed", type=float, default=60.0, help="Seconds of data per real second.")
    args = parser.parse_args()

    if KafkaProducer is None:
        raise SystemExit("kafka-python is required. Install with: pip install kafka-python")

    parquet_path = os.path.join(args.output, args.source, f"{args.dataset}.parquet")
    download_parquet(args.source, parquet_path)

    conn = duckdb.connect(database=":memory:")
    build_clean_view(conn, parquet_path, args.source)

    producer = KafkaProducer(bootstrap_servers=args.bootstrap)
    replay_events(iter_rows(conn), producer, args.topic, args.speed)


if __name__ == "__main__":
    main()
