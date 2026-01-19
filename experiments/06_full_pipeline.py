#!/usr/bin/env python3
"""
Experiment 06: Full Pipeline - Kafka to Iceberg

This experiment demonstrates the complete data pipeline:
Kafka (source) → Flink (processing) → Iceberg (sink)

This is a walkthrough of the main job with detailed explanations.

Prerequisites:
- All services running (docker compose up -d)
- JAR files downloaded
- Kafka topic 'trades' created
- Producer running

Usage:
    # This is primarily educational - read through the code
    # The actual job is in jobs/kafka_to_iceberg.py
"""


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def experiment_1_pipeline_overview():
    """
    Experiment 6.1: Pipeline Overview

    Understanding the complete data flow.
    """
    print_section("6.1 Pipeline Overview")

    print("""
    COMPLETE PIPELINE ARCHITECTURE:

    ┌─────────────────────────────────────────────────────────────────┐
    │                                                                  │
    │   DATA SOURCE                                                    │
    │   ┌──────────────┐                                              │
    │   │   Producer   │  Generates trade events every 200ms          │
    │   │   (Python)   │  {symbol, ts, price, qty, side}             │
    │   └──────┬───────┘                                              │
    │          │                                                       │
    │          ▼                                                       │
    │   ┌──────────────┐                                              │
    │   │    Kafka     │  Topic: trades, 3 partitions                 │
    │   │   (Broker)   │  Ordered by symbol (key)                     │
    │   └──────┬───────┘                                              │
    │          │                                                       │
    │   STREAM PROCESSING                                              │
    │          │                                                       │
    │          ▼                                                       │
    │   ┌──────────────┐                                              │
    │   │   PyFlink    │  1. Read from Kafka                          │
    │   │   (Job)      │  2. Parse JSON                               │
    │   │              │  3. Route to two sinks:                      │
    │   └──────┬───────┘                                              │
    │          │                                                       │
    │   DATA SINKS                                                     │
    │          │                                                       │
    │          ├─────────────────────────────┐                        │
    │          ▼                             ▼                        │
    │   ┌──────────────┐              ┌──────────────┐                │
    │   │  raw_trades  │              │   agg_1m     │                │
    │   │  (Iceberg)   │              │  (Iceberg)   │                │
    │   │              │              │              │                │
    │   │ All events   │              │ 1-min OHLCV  │                │
    │   │ append-only  │              │ by symbol    │                │
    │   └──────────────┘              └──────────────┘                │
    │          │                             │                        │
    │          ▼                             ▼                        │
    │   ┌─────────────────────────────────────────────┐               │
    │   │                   MinIO                      │               │
    │   │          s3a://warehouse/iceberg/            │               │
    │   └─────────────────────────────────────────────┘               │
    │                                                                  │
    └─────────────────────────────────────────────────────────────────┘
    """)


def experiment_2_kafka_source():
    """
    Experiment 6.2: Kafka Source Table

    Defining the data source.
    """
    print_section("6.2 Kafka Source Table Definition")

    print("""
    KAFKA SOURCE TABLE:

    ```sql
    CREATE TABLE kafka_trades (
        -- Data fields (from JSON)
        `symbol` STRING,           -- Trading pair: BTCUSDT, ETHUSDT, SOLUSDT
        `ts` BIGINT,               -- Unix timestamp in milliseconds
        `price` DOUBLE,            -- Trade price
        `qty` DOUBLE,              -- Trade quantity
        `side` STRING,             -- BUY or SELL

        -- Computed columns
        `event_time` AS TO_TIMESTAMP_LTZ(`ts`, 3),  -- Convert to timestamp
        `proc_time` AS PROCTIME(),                   -- Processing time

        -- Watermark for event time processing
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
    );
    ```

    KEY POINTS:

    1. COMPUTED COLUMNS:
       - event_time: Converts millisecond timestamp to TIMESTAMP
       - proc_time: Current processing time (for processing-time operations)

    2. WATERMARK:
       - Allows 5 seconds of late data
       - Required for event-time window operations

    3. CONNECTOR OPTIONS:
       - scan.startup.mode: 'earliest-offset' to read from beginning
       - json.ignore-parse-errors: Skip malformed messages
    """)


def experiment_3_iceberg_tables():
    """
    Experiment 6.3: Iceberg Sink Tables

    Defining the data destinations.
    """
    print_section("6.3 Iceberg Sink Table Definitions")

    print("""
    1. RAW TRADES TABLE (Append-only, all events):

    ```sql
    CREATE TABLE raw_trades (
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
    );
    ```

    2. 1-MINUTE AGGREGATION TABLE (OHLCV):

    ```sql
    CREATE TABLE agg_1m (
        `symbol` STRING,
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3),
        `open_price` DOUBLE,       -- First price in window
        `high_price` DOUBLE,       -- Max price
        `low_price` DOUBLE,        -- Min price
        `close_price` DOUBLE,      -- Last price in window
        `total_qty` DOUBLE,        -- Total volume
        `trade_count` BIGINT,      -- Number of trades
        `buy_qty` DOUBLE,          -- Buy volume
        `sell_qty` DOUBLE,         -- Sell volume
        PRIMARY KEY (`symbol`, `window_start`) NOT ENFORCED
    ) WITH (
        'format-version' = '2',
        'write.upsert.enabled' = 'true'
    );
    ```

    KEY POINTS:

    1. FORMAT VERSION 2:
       - Supports row-level deletes
       - Required for upsert operations

    2. UPSERT ENABLED:
       - Updates existing rows if primary key matches
       - Important for aggregation updates

    3. PRIMARY KEY:
       - NOT ENFORCED: Iceberg doesn't enforce uniqueness
       - Used for upsert matching
    """)


def experiment_4_insert_statements():
    """
    Experiment 6.4: INSERT Statements

    The actual data pipelines.
    """
    print_section("6.4 INSERT Statements (Pipelines)")

    print("""
    1. RAW DATA PIPELINE (Simple copy):

    ```sql
    INSERT INTO raw_trades
    SELECT
        symbol,
        ts,
        price,
        qty,
        side,
        event_time
    FROM kafka_trades;
    ```

    This creates a continuous pipeline that:
    - Reads from Kafka
    - Writes every event to Iceberg raw_trades table


    2. AGGREGATION PIPELINE (Window calculation):

    ```sql
    INSERT INTO agg_1m
    SELECT
        symbol,
        window_start,
        window_end,

        -- OHLC prices
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
    GROUP BY symbol, window_start, window_end;
    ```

    This creates a pipeline that:
    - Reads from Kafka
    - Groups into 1-minute tumbling windows
    - Calculates OHLCV for each symbol
    - Writes aggregated results to Iceberg
    """)


def experiment_5_statement_set():
    """
    Experiment 6.5: Statement Set for Multiple Sinks

    Running multiple pipelines efficiently.
    """
    print_section("6.5 Statement Set (Multiple Sinks)")

    print("""
    STATEMENT SET: Execute multiple INSERT statements together

    Without Statement Set (inefficient):
    ```python
    # Each creates a separate job, reads Kafka twice!
    t_env.execute_sql("INSERT INTO raw_trades SELECT ...")
    t_env.execute_sql("INSERT INTO agg_1m SELECT ...")
    ```

    With Statement Set (efficient):
    ```python
    stmt_set = t_env.create_statement_set()

    # Both share the same Kafka source!
    stmt_set.add_insert_sql("INSERT INTO raw_trades SELECT ...")
    stmt_set.add_insert_sql("INSERT INTO agg_1m SELECT ...")

    # Execute as a single job
    stmt_set.execute().wait()
    ```

    BENEFITS:
    ┌──────────────────────────────────────────────────────┐
    │                                                       │
    │       Kafka Source                                    │
    │            │                                          │
    │            ▼                                          │
    │    ┌───────────────┐                                 │
    │    │ Single Reader │   <- Read once                  │
    │    └───────────────┘                                 │
    │            │                                          │
    │      ┌─────┴─────┐                                   │
    │      ▼           ▼                                   │
    │  raw_trades   agg_1m     <- Write to both           │
    │                                                       │
    │  - Less network I/O                                  │
    │  - Less CPU usage                                    │
    │  - Consistent data (same source)                     │
    └──────────────────────────────────────────────────────┘
    """)


def experiment_6_verification():
    """
    Experiment 6.6: Verifying the Pipeline

    How to check that everything is working.
    """
    print_section("6.6 Verifying the Pipeline")

    print("""
    VERIFICATION CHECKLIST:

    1. CHECK FLINK WEB UI (http://localhost:8081):
       - Job status: RUNNING
       - No exceptions in logs
       - Checkpoints completing successfully
       - Records in/out increasing

    2. CHECK MINIO (http://localhost:9001):
       - warehouse/iceberg/lakehouse/raw_trades/data/ has .parquet files
       - warehouse/iceberg/lakehouse/agg_1m/data/ has .parquet files
       - Files growing over time

    3. CHECK VIA FLINK SQL CLIENT:
       ```bash
       docker exec -it flink-jobmanager ./bin/sql-client.sh
       ```

       ```sql
       -- Configure JARs
       SET 'pipeline.jars' = '...';

       -- Create catalog
       CREATE CATALOG iceberg_catalog WITH (...);
       USE CATALOG iceberg_catalog;
       USE lakehouse;

       -- Verify raw data
       SELECT COUNT(*) FROM raw_trades;  -- Should increase
       SELECT * FROM raw_trades LIMIT 5;

       -- Verify aggregations
       SELECT COUNT(*) FROM agg_1m;  -- Should increase every minute
       SELECT * FROM agg_1m ORDER BY window_start DESC LIMIT 5;
       ```

    4. CHECK PRODUCER OUTPUT:
       - Console shows "sent: BTCUSDT BUY 0.1234 @ 42150.00"
       - Messages going to different partitions

    5. COMMON ISSUES:
       - No data: Check Kafka connection, topic exists
       - Job failing: Check JAR files are loaded
       - No Iceberg writes: Check MinIO bucket exists
    """)


def experiment_7_code_walkthrough():
    """
    Experiment 6.7: Complete Code Walkthrough

    Step-by-step explanation of the job.
    """
    print_section("6.7 Complete Code Walkthrough")

    print("""
    JOBS/KAFKA_TO_ICEBERG.PY BREAKDOWN:

    ┌────────────────────────────────────────────────────────────────┐
    │  STEP 1: Create Execution Environment                          │
    │  ─────────────────────────────────────────────────────────────│
    │  env = StreamExecutionEnvironment.get_execution_environment()  │
    │  env.set_parallelism(2)                                        │
    │  env.enable_checkpointing(60000)  # Every 60 seconds          │
    │                                                                 │
    │  t_env = StreamTableEnvironment.create(env)                    │
    └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────────────┐
    │  STEP 2: Configure JARs                                         │
    │  ─────────────────────────────────────────────────────────────│
    │  t_env.get_config().set("pipeline.jars",                       │
    │      "iceberg-flink-runtime.jar;"                              │
    │      "flink-sql-connector-kafka.jar;"                          │
    │      "hadoop-aws.jar;"                                         │
    │      "aws-java-sdk-bundle.jar"                                 │
    │  )                                                              │
    └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────────────┐
    │  STEP 3: Create Iceberg Catalog                                 │
    │  ─────────────────────────────────────────────────────────────│
    │  CREATE CATALOG iceberg_catalog WITH (                         │
    │      'type' = 'iceberg',                                       │
    │      'catalog-type' = 'hadoop',                                │
    │      'warehouse' = 's3a://warehouse/iceberg',                  │
    │      ...MinIO connection settings...                           │
    │  );                                                             │
    │  CREATE DATABASE IF NOT EXISTS iceberg_catalog.lakehouse;      │
    └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────────────┐
    │  STEP 4: Create Kafka Source Table                              │
    │  ─────────────────────────────────────────────────────────────│
    │  CREATE TABLE kafka_trades (                                   │
    │      symbol STRING, ts BIGINT, price DOUBLE, qty DOUBLE,       │
    │      side STRING, event_time AS ..., WATERMARK ...             │
    │  ) WITH ('connector' = 'kafka', ...)                           │
    └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────────────┐
    │  STEP 5: Create Iceberg Sink Tables                             │
    │  ─────────────────────────────────────────────────────────────│
    │  CREATE TABLE raw_trades (...);                                │
    │  CREATE TABLE agg_1m (...);                                    │
    └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────────────┐
    │  STEP 6: Create and Execute Statement Set                       │
    │  ─────────────────────────────────────────────────────────────│
    │  stmt_set = t_env.create_statement_set()                       │
    │  stmt_set.add_insert_sql("INSERT INTO raw_trades ...")         │
    │  stmt_set.add_insert_sql("INSERT INTO agg_1m ...")             │
    │  stmt_set.execute().wait()  # Blocks until job stops           │
    └────────────────────────────────────────────────────────────────┘

    The job runs continuously until cancelled!
    """)


def main():
    """Run all full pipeline experiments."""
    print("\n" + "#" * 60)
    print("#  Full Pipeline: Kafka → Flink → Iceberg")
    print("#" * 60)

    print("""
    This script explains the complete data pipeline:

    6.1 Pipeline Overview    - Architecture diagram
    6.2 Kafka Source         - Source table definition
    6.3 Iceberg Tables       - Sink table definitions
    6.4 INSERT Statements    - Pipeline definitions
    6.5 Statement Set        - Multi-sink efficiency
    6.6 Verification         - Testing the pipeline
    6.7 Code Walkthrough     - Step-by-step explanation

    This is educational - the actual job is in jobs/kafka_to_iceberg.py
    """)

    input("Press Enter to start the walkthrough...")

    experiment_1_pipeline_overview()
    input("\nPress Enter to continue...")

    experiment_2_kafka_source()
    input("\nPress Enter to continue...")

    experiment_3_iceberg_tables()
    input("\nPress Enter to continue...")

    experiment_4_insert_statements()
    input("\nPress Enter to continue...")

    experiment_5_statement_set()
    input("\nPress Enter to continue...")

    experiment_6_verification()
    input("\nPress Enter to continue...")

    experiment_7_code_walkthrough()

    print("\n" + "#" * 60)
    print("#  Full pipeline walkthrough completed!")
    print("#")
    print("#  Next steps:")
    print("#  1. Run: docker compose up -d")
    print("#  2. Run: python producer/producer.py")
    print("#  3. Submit job: docker exec flink-jobmanager flink run -py ...")
    print("#  4. Verify in Flink UI and MinIO Console")
    print("#" * 60)


if __name__ == "__main__":
    main()
