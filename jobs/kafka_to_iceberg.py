#!/usr/bin/env python3
"""
PyFlink Job: Kafka to Iceberg Pipeline

This job reads trade events from Kafka and writes them to Iceberg tables:
1. raw_trades: Raw trade data (append-only)
2. agg_1m: 1-minute window aggregations (symbol-level metrics)

Architecture:
    Kafka (trades topic)
        -> PyFlink (stream processing)
            -> Iceberg (raw_trades): All trade events
            -> Iceberg (agg_1m): 1-min OHLCV aggregations

Usage:
    # Submit via Flink CLI
    docker exec -it flink-jobmanager flink run -py /opt/flink/usrlib/jobs/kafka_to_iceberg.py
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main():
    """Main entry point for the PyFlink job."""

    # ===========================================================
    # 1. Create execution environment
    # ===========================================================
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Enable checkpointing for exactly-once semantics
    env.enable_checkpointing(60000)  # checkpoint every 60 seconds

    # Create Table environment
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Configure state TTL for aggregations
    t_env.get_config().set("table.exec.state.ttl", "86400000")  # 24 hours

    # ===========================================================
    # 2. Add JAR dependencies
    # ===========================================================
    # Note: These JARs should be in /opt/flink/lib/extra (mounted from ./jars)
    jar_files = [
        "file:///opt/flink/lib/extra/iceberg-flink-runtime-1.19-1.7.0.jar",
        "file:///opt/flink/lib/extra/flink-sql-connector-kafka-3.2.0-1.19.jar",
        "file:///opt/flink/lib/extra/hadoop-aws-3.3.4.jar",
        "file:///opt/flink/lib/extra/aws-java-sdk-bundle-1.12.648.jar",
    ]

    for jar in jar_files:
        t_env.get_config().set("pipeline.jars", ";".join(jar_files))

    # ===========================================================
    # 3. Create Iceberg Catalog
    # ===========================================================
    t_env.execute_sql("""
        CREATE CATALOG iceberg_catalog WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = 's3a://warehouse/iceberg',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://minio:9000',
            's3.access-key-id' = 'minioadmin',
            's3.secret-access-key' = 'minioadmin',
            's3.path-style-access' = 'true'
        )
    """)

    # Create database
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.lakehouse")
    t_env.use_catalog("iceberg_catalog")
    t_env.use_database("lakehouse")

    # ===========================================================
    # 4. Create Kafka Source Table
    # ===========================================================
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS kafka_trades (
            `symbol` STRING,
            `ts` BIGINT,
            `price` DOUBLE,
            `qty` DOUBLE,
            `side` STRING,
            `event_time` AS TO_TIMESTAMP_LTZ(`ts`, 3),
            `proc_time` AS PROCTIME(),
            WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'trades',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-iceberg-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ===========================================================
    # 5. Create Iceberg Raw Trades Table
    # ===========================================================
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS raw_trades (
            `symbol` STRING,
            `ts` BIGINT,
            `price` DOUBLE,
            `qty` DOUBLE,
            `side` STRING,
            `event_time` TIMESTAMP_LTZ(3),
            PRIMARY KEY (`symbol`, `ts`) NOT ENFORCED
        ) WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)

    # ===========================================================
    # 6. Create Iceberg 1-Minute Aggregation Table
    # ===========================================================
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS agg_1m (
            `symbol` STRING,
            `window_start` TIMESTAMP(3),
            `window_end` TIMESTAMP(3),
            `open_price` DOUBLE,
            `high_price` DOUBLE,
            `low_price` DOUBLE,
            `close_price` DOUBLE,
            `total_qty` DOUBLE,
            `trade_count` BIGINT,
            `buy_qty` DOUBLE,
            `sell_qty` DOUBLE,
            PRIMARY KEY (`symbol`, `window_start`) NOT ENFORCED
        ) WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)

    # ===========================================================
    # 7. Create Statement Set for Multiple Inserts
    # ===========================================================
    stmt_set = t_env.create_statement_set()

    # Insert raw trades
    stmt_set.add_insert_sql("""
        INSERT INTO raw_trades
        SELECT
            symbol,
            ts,
            price,
            qty,
            side,
            event_time
        FROM kafka_trades
    """)

    # Insert 1-minute aggregations
    stmt_set.add_insert_sql("""
        INSERT INTO agg_1m
        SELECT
            symbol,
            window_start,
            window_end,
            -- OHLC prices (using FIRST_VALUE/LAST_VALUE with ordering)
            FIRST_VALUE(price) AS open_price,
            MAX(price) AS high_price,
            MIN(price) AS low_price,
            LAST_VALUE(price) AS close_price,
            -- Volume metrics
            SUM(qty) AS total_qty,
            COUNT(*) AS trade_count,
            SUM(CASE WHEN side = 'BUY' THEN qty ELSE 0 END) AS buy_qty,
            SUM(CASE WHEN side = 'SELL' THEN qty ELSE 0 END) AS sell_qty
        FROM TABLE(
            TUMBLE(TABLE kafka_trades, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY symbol, window_start, window_end
    """)

    # ===========================================================
    # 8. Execute the pipeline
    # ===========================================================
    print("=" * 60)
    print("Starting Kafka to Iceberg Pipeline")
    print("=" * 60)
    print("Source: Kafka topic 'trades'")
    print("Sinks:")
    print("  - iceberg_catalog.lakehouse.raw_trades (raw events)")
    print("  - iceberg_catalog.lakehouse.agg_1m (1-min OHLCV)")
    print("=" * 60)

    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
