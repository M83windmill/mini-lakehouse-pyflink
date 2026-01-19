#!/usr/bin/env python3
"""
Experiment 04: Iceberg Basics

This experiment teaches Apache Iceberg concepts:
- What is a table format
- Iceberg architecture (catalog, metadata, data files)
- Snapshots and time travel
- Schema evolution
- Hidden partitioning

Prerequisites:
- Flink and MinIO are running (docker compose up -d)
- JAR files downloaded

Usage:
    docker exec -it flink-jobmanager python /tmp/04_iceberg_basics.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def create_environment():
    """Create table environment with Iceberg support."""
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()

    t_env = TableEnvironment.create(settings)

    # Add JAR dependencies
    jars = [
        "file:///opt/flink/lib/extra/iceberg-flink-runtime-1.19-1.7.0.jar",
        "file:///opt/flink/lib/extra/hadoop-aws-3.3.4.jar",
        "file:///opt/flink/lib/extra/aws-java-sdk-bundle-1.12.648.jar",
    ]
    t_env.get_config().set("pipeline.jars", ";".join(jars))

    return t_env


def experiment_1_what_is_iceberg():
    """
    Experiment 4.1: What is Apache Iceberg?

    Understanding the problem Iceberg solves.
    """
    print_section("4.1 What is Apache Iceberg?")

    print("""
    PROBLEM: Storing large tables on object storage (S3/MinIO)

    Traditional approach: Just use Parquet/ORC files
    ┌──────────────────────────────────────────────────────┐
    │  s3://bucket/data/                                   │
    │  ├── part-00000.parquet                              │
    │  ├── part-00001.parquet                              │
    │  └── ...                                             │
    │                                                       │
    │  Problems:                                           │
    │  - No ACID transactions                              │
    │  - No schema evolution                               │
    │  - Must scan ALL files for queries                   │
    │  - Concurrent writes corrupt data                    │
    │  - No time travel / rollback                         │
    └──────────────────────────────────────────────────────┘

    SOLUTION: Iceberg Table Format

    Iceberg adds a metadata layer that tracks:
    - Which files belong to the table
    - Schema information
    - Partition information
    - Snapshots (versions) of the table

    Benefits:
    ✓ ACID transactions (safe concurrent access)
    ✓ Schema evolution (add/rename/drop columns)
    ✓ Hidden partitioning (no partition columns in queries)
    ✓ Time travel (query historical data)
    ✓ Efficient queries (skip irrelevant files)
    """)


def experiment_2_iceberg_architecture():
    """
    Experiment 4.2: Iceberg Architecture

    Understanding the layered structure.
    """
    print_section("4.2 Iceberg Architecture")

    print("""
    ICEBERG ARCHITECTURE: Three layers

    ┌─────────────────────────────────────────────────────────┐
    │                      CATALOG                            │
    │  (Tracks table locations: Hadoop, Hive, REST, etc.)     │
    │                          │                              │
    │                          ▼                              │
    │  ┌───────────────────────────────────────────────────┐  │
    │  │               METADATA LAYER                       │  │
    │  │                                                    │  │
    │  │  metadata/                                         │  │
    │  │  ├── v1.metadata.json  (snapshot 1)               │  │
    │  │  ├── v2.metadata.json  (snapshot 2)  ← current    │  │
    │  │  └── v3.metadata.json  (snapshot 3)               │  │
    │  │                                                    │  │
    │  │  Each metadata file points to manifest lists       │  │
    │  └───────────────────────────────────────────────────┘  │
    │                          │                              │
    │                          ▼                              │
    │  ┌───────────────────────────────────────────────────┐  │
    │  │                 DATA LAYER                         │  │
    │  │                                                    │  │
    │  │  data/                                             │  │
    │  │  ├── 00000-0-xxxx.parquet                         │  │
    │  │  ├── 00001-0-yyyy.parquet                         │  │
    │  │  └── 00002-0-zzzz.parquet                         │  │
    │  │                                                    │  │
    │  │  Actual data in Parquet/ORC/Avro format           │  │
    │  └───────────────────────────────────────────────────┘  │
    └─────────────────────────────────────────────────────────┘

    Key insight: Data files never change!
    - Inserts create new files
    - Updates/deletes create new snapshots pointing to different files
    """)


def experiment_3_create_catalog(t_env: TableEnvironment):
    """
    Experiment 4.3: Create Iceberg Catalog

    Setting up connection to Iceberg tables in MinIO.
    """
    print_section("4.3 Creating Iceberg Catalog")

    # Create Hadoop catalog pointing to MinIO
    t_env.execute_sql("""
        CREATE CATALOG iceberg_demo WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = 's3a://warehouse/iceberg_demo',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://minio:9000',
            's3.access-key-id' = 'minioadmin',
            's3.secret-access-key' = 'minioadmin',
            's3.path-style-access' = 'true'
        )
    """)

    print("Iceberg catalog 'iceberg_demo' created")
    print("""
    Catalog Configuration:
    - type: iceberg
    - catalog-type: hadoop (file-based, simple)
    - warehouse: s3a://warehouse/iceberg_demo
    - Using MinIO as S3-compatible storage
    """)

    # Create database
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_demo.experiments")
    print("\nDatabase 'experiments' created")

    return t_env


def experiment_4_create_table(t_env: TableEnvironment):
    """
    Experiment 4.4: Create Iceberg Table

    Creating a table with schema and properties.
    """
    print_section("4.4 Creating Iceberg Table")

    t_env.use_catalog("iceberg_demo")
    t_env.use_database("experiments")

    # Create an Iceberg table
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS sample_trades (
            trade_id BIGINT,
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            trade_time TIMESTAMP(3),
            PRIMARY KEY (trade_id) NOT ENFORCED
        ) WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)

    print("Table 'sample_trades' created")
    print("""
    Table Properties:
    - format-version = 2: Latest Iceberg format (supports row-level deletes)
    - write.upsert.enabled = true: Allow upsert operations

    Schema:
    - trade_id: BIGINT (primary key)
    - symbol: STRING
    - price: DOUBLE
    - quantity: DOUBLE
    - trade_time: TIMESTAMP
    """)


def experiment_5_snapshots():
    """
    Experiment 4.5: Snapshots and Time Travel

    Understanding versioning in Iceberg.
    """
    print_section("4.5 Snapshots and Time Travel")

    print("""
    SNAPSHOTS: Versions of your table

    Every write creates a new snapshot:
    ┌─────────────────────────────────────────────────────────┐
    │                                                          │
    │  Snapshot 1        Snapshot 2        Snapshot 3         │
    │  (Initial load)    (+ 1000 rows)     (Update 50 rows)   │
    │       │                 │                 │              │
    │       ▼                 ▼                 ▼              │
    │  ┌────────┐        ┌────────┐        ┌────────┐         │
    │  │ File A │        │ File A │        │ File A │         │
    │  └────────┘        │ File B │        │ File B'│ (new)   │
    │                    └────────┘        └────────┘         │
    │                                                          │
    │  Snapshot 1: [A]                                         │
    │  Snapshot 2: [A, B]                                      │
    │  Snapshot 3: [A, B']  (B' replaces some rows from B)     │
    └─────────────────────────────────────────────────────────┘

    TIME TRAVEL: Query any snapshot

    SQL Examples:
    ```sql
    -- Query specific snapshot by ID
    SELECT * FROM sample_trades VERSION AS OF 123456789;

    -- Query specific time
    SELECT * FROM sample_trades TIMESTAMP AS OF '2024-01-15 10:00:00';

    -- View all snapshots
    SELECT * FROM iceberg_demo.experiments.`sample_trades$snapshots`;

    -- View table history
    SELECT * FROM iceberg_demo.experiments.`sample_trades$history`;
    ```

    Use cases:
    - Audit: See what data looked like at a specific time
    - Recovery: Rollback to a previous good state
    - Debugging: Compare data between versions
    """)


def experiment_6_schema_evolution():
    """
    Experiment 4.6: Schema Evolution

    Changing table schema without rewriting data.
    """
    print_section("4.6 Schema Evolution")

    print("""
    SCHEMA EVOLUTION: Change schema without rewriting data

    Iceberg supports these schema changes:
    ┌──────────────────────────────────────────────────────┐
    │  Operation        │ Syntax                           │
    ├───────────────────┼──────────────────────────────────┤
    │  Add column       │ ALTER TABLE t ADD COLUMN col INT │
    │  Drop column      │ ALTER TABLE t DROP COLUMN col    │
    │  Rename column    │ ALTER TABLE t RENAME col TO new  │
    │  Reorder columns  │ ALTER TABLE t ALTER COLUMN ...   │
    │  Widen type       │ ALTER TABLE t ALTER col TYPE ... │
    └──────────────────────────────────────────────────────┘

    Example:
    ```sql
    -- Add a new column
    ALTER TABLE sample_trades ADD COLUMN exchange STRING;

    -- Existing data: new column will be NULL
    -- New data: can include the new column
    ```

    Why this matters:
    - No need to rewrite terabytes of data
    - Existing queries keep working
    - Schema changes are tracked in metadata
    """)


def experiment_7_partitioning():
    """
    Experiment 4.7: Hidden Partitioning

    Efficient data organization without partition columns.
    """
    print_section("4.7 Hidden Partitioning")

    print("""
    HIDDEN PARTITIONING: Automatic, user-transparent partitioning

    Traditional (Hive-style) partitioning:
    ┌──────────────────────────────────────────────────────┐
    │  -- Must include partition column in queries         │
    │  SELECT * FROM trades                                │
    │  WHERE year='2024' AND month='01' AND day='15';     │
    │                                                       │
    │  Files:                                              │
    │  s3://bucket/trades/year=2024/month=01/day=15/...   │
    └──────────────────────────────────────────────────────┘

    Iceberg hidden partitioning:
    ┌──────────────────────────────────────────────────────┐
    │  -- Just use the timestamp, Iceberg figures it out   │
    │  SELECT * FROM trades                                │
    │  WHERE trade_time >= '2024-01-15'                    │
    │    AND trade_time < '2024-01-16';                    │
    │                                                       │
    │  CREATE TABLE trades (                               │
    │    ...                                                │
    │    trade_time TIMESTAMP                              │
    │  ) PARTITIONED BY (days(trade_time));               │
    └──────────────────────────────────────────────────────┘

    Partition transforms:
    - years(ts)    → partition by year
    - months(ts)   → partition by month
    - days(ts)     → partition by day
    - hours(ts)    → partition by hour
    - bucket(n, col) → hash into n buckets
    - truncate(w, col) → truncate to width

    Benefits:
    - Queries don't need to know about partitions
    - Can change partitioning without rewriting data
    - Better performance with automatic pruning
    """)


def main():
    """Run all Iceberg experiments."""
    print("\n" + "#" * 60)
    print("#  Apache Iceberg Basics Experiments")
    print("#" * 60)

    print("""
    This script demonstrates Apache Iceberg concepts:

    4.1 What is Iceberg?    - Problem and solution
    4.2 Architecture        - Catalog, metadata, data layers
    4.3 Create Catalog      - Connect to MinIO storage
    4.4 Create Table        - Define Iceberg table
    4.5 Snapshots           - Versioning and time travel
    4.6 Schema Evolution    - Change schema safely
    4.7 Partitioning        - Hidden partitioning benefits
    """)

    input("Press Enter to start the experiments...")

    experiment_1_what_is_iceberg()
    input("\nPress Enter to continue...")

    experiment_2_iceberg_architecture()
    input("\nPress Enter to continue...")

    # Create environment and catalog
    t_env = create_environment()
    experiment_3_create_catalog(t_env)
    input("\nPress Enter to continue...")

    experiment_4_create_table(t_env)
    input("\nPress Enter to continue...")

    experiment_5_snapshots()
    input("\nPress Enter to continue...")

    experiment_6_schema_evolution()
    input("\nPress Enter to continue...")

    experiment_7_partitioning()

    print("\n" + "#" * 60)
    print("#  All Iceberg experiments completed!")
    print("#" * 60)


if __name__ == "__main__":
    main()
