-- ============================================================
-- Verification SQL Statements for Flink SQL Client
-- ============================================================
-- Usage:
--   docker exec -it flink-jobmanager ./bin/sql-client.sh
--   Then copy-paste these statements to verify the pipeline
-- ============================================================

-- ------------------------------------------------------------
-- Step 1: Configure JAR dependencies
-- ------------------------------------------------------------
-- Note: Run these SET statements first to load the connectors

SET 'pipeline.jars' = 'file:///opt/flink/lib/extra/iceberg-flink-runtime-1.19-1.7.0.jar;file:///opt/flink/lib/extra/flink-sql-connector-kafka-3.2.0-1.19.jar;file:///opt/flink/lib/extra/hadoop-aws-3.3.4.jar;file:///opt/flink/lib/extra/aws-java-sdk-bundle-1.12.648.jar';

-- ------------------------------------------------------------
-- Step 2: Create Iceberg Catalog
-- ------------------------------------------------------------
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 's3a://warehouse/iceberg',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key-id' = 'minioadmin',
    's3.secret-access-key' = 'minioadmin',
    's3.path-style-access' = 'true'
);

USE CATALOG iceberg_catalog;
USE lakehouse;

-- ------------------------------------------------------------
-- Step 3: Verify Raw Trades Table
-- ------------------------------------------------------------

-- Check row count (should increase over time)
SELECT COUNT(*) AS total_trades FROM raw_trades;

-- View recent trades
SELECT * FROM raw_trades ORDER BY ts DESC LIMIT 10;

-- Check trades by symbol
SELECT
    symbol,
    COUNT(*) AS trade_count,
    SUM(qty) AS total_volume,
    AVG(price) AS avg_price
FROM raw_trades
GROUP BY symbol;

-- Check trades by side
SELECT
    side,
    COUNT(*) AS trade_count,
    SUM(qty) AS total_volume
FROM raw_trades
GROUP BY side;

-- ------------------------------------------------------------
-- Step 4: Verify 1-Minute Aggregation Table
-- ------------------------------------------------------------

-- Check aggregation count
SELECT COUNT(*) AS total_windows FROM agg_1m;

-- View recent aggregations
SELECT * FROM agg_1m ORDER BY window_start DESC LIMIT 10;

-- Check OHLCV by symbol
SELECT
    symbol,
    window_start,
    open_price,
    high_price,
    low_price,
    close_price,
    total_qty,
    trade_count
FROM agg_1m
ORDER BY window_start DESC
LIMIT 20;

-- Check buy/sell volume distribution
SELECT
    symbol,
    window_start,
    buy_qty,
    sell_qty,
    (buy_qty / (buy_qty + sell_qty)) * 100 AS buy_pct
FROM agg_1m
WHERE buy_qty + sell_qty > 0
ORDER BY window_start DESC
LIMIT 10;

-- ------------------------------------------------------------
-- Step 5: Verify Kafka Source (optional)
-- ------------------------------------------------------------

-- Create Kafka source table for testing
CREATE TEMPORARY TABLE kafka_verify (
    `symbol` STRING,
    `ts` BIGINT,
    `price` DOUBLE,
    `qty` DOUBLE,
    `side` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'trades',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'verify-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- Read a few messages from Kafka
SELECT * FROM kafka_verify LIMIT 5;

-- ------------------------------------------------------------
-- Step 6: Iceberg Metadata Queries
-- ------------------------------------------------------------

-- View table snapshots (time travel)
SELECT * FROM iceberg_catalog.lakehouse.`raw_trades$snapshots`;

-- View table manifests
SELECT * FROM iceberg_catalog.lakehouse.`raw_trades$manifests`;

-- View table history
SELECT * FROM iceberg_catalog.lakehouse.`raw_trades$history`;

-- View data files
SELECT * FROM iceberg_catalog.lakehouse.`raw_trades$files`;
