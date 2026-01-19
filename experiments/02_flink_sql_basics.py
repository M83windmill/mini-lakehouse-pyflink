#!/usr/bin/env python3
"""
Experiment 02: Flink SQL Basics

This experiment teaches fundamental Flink SQL concepts:
- Creating a table environment
- Defining tables with DDL
- Basic SELECT queries
- INSERT INTO operations

Prerequisites:
- Flink is running (docker compose up -d)
- JAR files downloaded (scripts/download_jars.ps1)

Usage:
    # Run inside Flink container
    docker exec -it flink-jobmanager python /opt/flink/usrlib/jobs/../experiments/02_flink_sql_basics.py

    # Or copy to container first
    docker cp experiments/02_flink_sql_basics.py flink-jobmanager:/tmp/
    docker exec -it flink-jobmanager python /tmp/02_flink_sql_basics.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def experiment_1_table_environment():
    """
    Experiment 2.1: Create Table Environment

    The TableEnvironment is the entry point for Flink SQL.
    It can run in batch or streaming mode.
    """
    print_section("2.1 Creating Table Environment")

    # Create a streaming table environment
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()

    t_env = TableEnvironment.create(settings)

    print("Table environment created in STREAMING mode")
    print(f"  - Current catalog: {t_env.get_current_catalog()}")
    print(f"  - Current database: {t_env.get_current_database()}")

    return t_env


def experiment_2_create_source_table(t_env: TableEnvironment):
    """
    Experiment 2.2: Create Source Table with DDL

    Tables are defined using CREATE TABLE statements.
    A source table reads data from an external system.
    """
    print_section("2.2 Creating Source Table (Datagen)")

    # Create a datagen source table for testing
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS datagen_source (
            id BIGINT,
            name STRING,
            amount DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '5',
            'fields.id.min' = '1',
            'fields.id.max' = '1000',
            'fields.name.length' = '10',
            'fields.amount.min' = '0',
            'fields.amount.max' = '100'
        )
    """)

    print("Source table 'datagen_source' created")
    print("""
    Table Definition:
    - id: BIGINT         - Auto-generated ID
    - name: STRING       - Random 10-char string
    - amount: DOUBLE     - Random value 0-100
    - event_time: TIMESTAMP - Event timestamp with watermark
    """)


def experiment_3_create_sink_table(t_env: TableEnvironment):
    """
    Experiment 2.3: Create Sink Table

    A sink table writes data to an external system.
    Using 'print' connector for demonstration.
    """
    print_section("2.3 Creating Sink Table (Print)")

    # Create a print sink table
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS print_sink (
            id BIGINT,
            name STRING,
            amount DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """)

    print("Sink table 'print_sink' created")
    print("  - Will print rows to stdout")


def experiment_4_basic_queries(t_env: TableEnvironment):
    """
    Experiment 2.4: Basic SELECT Queries

    Flink SQL supports standard SQL queries.
    Use table.execute() to run and collect results.
    """
    print_section("2.4 Basic SELECT Queries")

    # Simple SELECT
    print("\n1. Simple SELECT (first 5 rows):")
    result = t_env.execute_sql("""
        SELECT id, name, amount
        FROM datagen_source
        LIMIT 5
    """)

    # Print schema
    print(f"\nSchema: {result.get_result_kind()}")

    # Collect and print rows
    with result.collect() as results:
        for row in results:
            print(f"  Row: id={row[0]}, name={row[1]}, amount={row[2]:.2f}")


def experiment_5_transformations(t_env: TableEnvironment):
    """
    Experiment 2.5: Data Transformations

    Flink SQL supports various transformation functions.
    """
    print_section("2.5 Data Transformations")

    # Transformation query
    print("\nTransformation example (first 5 rows):")
    result = t_env.execute_sql("""
        SELECT
            id,
            UPPER(name) AS name_upper,
            ROUND(amount, 1) AS amount_rounded,
            CASE
                WHEN amount < 33 THEN 'LOW'
                WHEN amount < 66 THEN 'MEDIUM'
                ELSE 'HIGH'
            END AS category
        FROM datagen_source
        LIMIT 5
    """)

    with result.collect() as results:
        for row in results:
            print(f"  Row: id={row[0]}, name={row[1]}, amount={row[2]}, category={row[3]}")


def experiment_6_insert_into(t_env: TableEnvironment):
    """
    Experiment 2.6: INSERT INTO Operation

    INSERT INTO writes query results to a sink table.
    This is how you create data pipelines.
    """
    print_section("2.6 INSERT INTO (Pipeline)")

    print("""
    The INSERT INTO statement creates a continuous pipeline:

        Source Table  ──────►  Transformations  ──────►  Sink Table
        (datagen)              (SELECT, filter)          (print)

    Example:
        INSERT INTO print_sink
        SELECT id, name, amount
        FROM datagen_source
        WHERE amount > 50

    In streaming mode, this runs continuously until cancelled.
    For this demo, we'll just show the syntax.
    """)

    # Create a limited sink for demo
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS blackhole_sink (
            id BIGINT,
            name STRING,
            amount DOUBLE
        ) WITH (
            'connector' = 'blackhole'
        )
    """)

    print("\nExecuting limited INSERT (runs briefly then returns):")

    # Note: In real streaming, this would run forever
    # Using blackhole connector which just discards data
    t_env.execute_sql("""
        INSERT INTO blackhole_sink
        SELECT id, name, amount
        FROM datagen_source
    """)

    print("  Pipeline submitted (writes to blackhole)")


def experiment_7_table_api(t_env: TableEnvironment):
    """
    Experiment 2.7: Table API (Alternative to SQL)

    Flink also has a Table API for programmatic queries.
    You can mix Table API and SQL.
    """
    print_section("2.7 Table API (Programmatic)")

    # Get table from SQL
    source_table = t_env.from_path("datagen_source")

    print("Table API example:")
    print("  1. Get table: t_env.from_path('datagen_source')")
    print("  2. Filter:    table.where(col('amount') > 50)")
    print("  3. Select:    table.select(col('id'), col('name'))")
    print("  4. Execute:   table.execute()")

    # Show table schema
    print(f"\nTable Schema:")
    source_table.print_schema()


def main():
    """Run all Flink SQL experiments."""
    print("\n" + "#" * 60)
    print("#  Flink SQL Basics Experiments")
    print("#" * 60)

    print("""
    This script demonstrates fundamental Flink SQL concepts:

    2.1 Table Environment  - Entry point for Flink SQL
    2.2 Source Tables      - Define data sources
    2.3 Sink Tables        - Define data destinations
    2.4 Basic Queries      - SELECT statements
    2.5 Transformations    - Data transformations
    2.6 INSERT INTO        - Create pipelines
    2.7 Table API          - Programmatic alternative
    """)

    input("Press Enter to start the experiments...")

    # Create environment
    t_env = experiment_1_table_environment()
    input("\nPress Enter to continue...")

    # Create tables
    experiment_2_create_source_table(t_env)
    input("\nPress Enter to continue...")

    experiment_3_create_sink_table(t_env)
    input("\nPress Enter to continue...")

    # Run queries
    experiment_4_basic_queries(t_env)
    input("\nPress Enter to continue...")

    experiment_5_transformations(t_env)
    input("\nPress Enter to continue...")

    experiment_6_insert_into(t_env)
    input("\nPress Enter to continue...")

    experiment_7_table_api(t_env)

    print("\n" + "#" * 60)
    print("#  All Flink SQL experiments completed!")
    print("#" * 60)


if __name__ == "__main__":
    main()
