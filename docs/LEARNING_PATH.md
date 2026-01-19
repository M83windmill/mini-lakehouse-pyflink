# Mini Lakehouse Lab - Learning Path

This document provides a structured learning path for understanding the streaming data lakehouse architecture.

## Table of Contents

1. [Chapter 1: Kafka Fundamentals](#chapter-1-kafka-fundamentals)
2. [Chapter 2: Flink Stream Processing](#chapter-2-flink-stream-processing)
3. [Chapter 3: Flink SQL Deep Dive](#chapter-3-flink-sql-deep-dive)
4. [Chapter 4: Apache Iceberg](#chapter-4-apache-iceberg)
5. [Chapter 5: Complete Pipeline](#chapter-5-complete-pipeline)

---

## Chapter 1: Kafka Fundamentals

### 1.1 What is Kafka?

**Analogy**: Think of Kafka as a **conveyor belt system** in a factory.

```
Producers (machines)     Kafka (conveyor belt)     Consumers (workers)
     │                         │                         │
     ▼                         ▼                         ▼
  ┌─────┐                 ┌─────────┐                ┌─────┐
  │ App │ ──── data ────► │ Topics  │ ──── data ──► │ App │
  │  A  │                 │(ordered)│                │  B  │
  └─────┘                 └─────────┘                └─────┘
```

**Key Characteristics**:
- **Durable**: Messages are stored on disk, not lost
- **Ordered**: Messages maintain order within a partition
- **Scalable**: Add more partitions = more throughput
- **Decoupled**: Producers and consumers don't need to know each other

### 1.2 Core Concepts

#### Topics
A **topic** is a category/feed name to which messages are published.

```
Topic: "trades"
├── Message 1: {"symbol": "BTC", "price": 42000}
├── Message 2: {"symbol": "ETH", "price": 2500}
└── Message 3: {"symbol": "BTC", "price": 42100}
```

#### Partitions
Topics are split into **partitions** for parallelism.

```
Topic: "trades" (3 partitions)

Partition 0: [msg1] [msg4] [msg7] ...
Partition 1: [msg2] [msg5] [msg8] ...
Partition 2: [msg3] [msg6] [msg9] ...
```

**Key points**:
- Messages with the same key go to the same partition
- Order is guaranteed within a partition, not across partitions
- Each partition can be consumed by one consumer in a group

#### Consumer Groups
A **consumer group** is a set of consumers that share the workload.

```
Topic (3 partitions)          Consumer Group "analytics"
┌─────────────────┐           ┌─────────────────┐
│  Partition 0    │ ────────► │   Consumer A    │
│  Partition 1    │ ────────► │                 │
├─────────────────┤           ├─────────────────┤
│  Partition 2    │ ────────► │   Consumer B    │
└─────────────────┘           └─────────────────┘
```

### 1.3 Hands-on Experiment

Run the Kafka basics experiment:
```bash
pip install kafka-python
python experiments/01_kafka_basics.py
```

**Learning objectives**:
- Create a topic
- Send messages with keys
- Consume messages
- Observe partition distribution

---

## Chapter 2: Flink Stream Processing

### 2.1 What is Apache Flink?

**Analogy**: Flink is like a **continuous assembly line** that processes items one by one as they arrive.

```
Traditional (Batch):        Streaming (Flink):
────────────────────        ────────────────────
Wait for all data           Process as data arrives
     │                           │ │ │
     ▼                           ▼ ▼ ▼
┌──────────────┐           ┌──────────────┐
│ Process all  │           │ Process each │
│   at once    │           │  immediately │
└──────────────┘           └──────────────┘
     │                           │ │ │
     ▼                           ▼ ▼ ▼
  Results                   Continuous results
 (once/day)                  (real-time)
```

### 2.2 Key Concepts

#### DataStream vs Table API

Flink offers two main APIs:

| Feature | DataStream API | Table API / SQL |
|---------|---------------|-----------------|
| Abstraction | Low-level | High-level |
| Flexibility | Maximum | Good |
| Learning curve | Steep | Gentle |
| Use case | Complex logic | SQL-like operations |

**This project uses Table API / SQL** for simplicity.

#### Event Time vs Processing Time

```
Event Time: When the event actually happened
            (embedded in data)

Processing Time: When Flink processes the event
                 (wall clock time)

Example:
- Sensor reads temperature at 10:00:00 (event time)
- Network delay, arrives at Flink at 10:00:05
- Processing time = 10:00:05
- Event time = 10:00:00
```

**Best practice**: Use event time for accurate analytics.

### 2.3 Checkpointing

Checkpointing enables **fault tolerance**.

```
┌─────────────────────────────────────────────────────────┐
│                     Flink Job                           │
│                                                          │
│  ┌─────┐     ┌─────┐     ┌─────┐                       │
│  │ Op1 │ ──► │ Op2 │ ──► │ Op3 │                       │
│  └──┬──┘     └──┬──┘     └──┬──┘                       │
│     │           │           │                           │
│     ▼           ▼           ▼                           │
│  [state]     [state]     [state]                       │
│     │           │           │                           │
│     └───────────┼───────────┘                           │
│                 │                                        │
│                 ▼                                        │
│         Checkpoint Storage                               │
│    (saves state periodically)                           │
└─────────────────────────────────────────────────────────┘

If failure occurs:
1. Job restarts
2. Restores from last checkpoint
3. Continues processing
4. No data loss!
```

### 2.4 Hands-on Experiment

Run the Flink SQL basics experiment:
```bash
docker exec -it flink-jobmanager python /tmp/02_flink_sql_basics.py
```

---

## Chapter 3: Flink SQL Deep Dive

### 3.1 Table Definition (DDL)

Tables are defined using `CREATE TABLE`:

```sql
CREATE TABLE kafka_trades (
    -- Data columns
    symbol STRING,
    price DOUBLE,
    qty DOUBLE,
    ts BIGINT,

    -- Computed column (transforms ts to timestamp)
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),

    -- Watermark for event time processing
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'trades',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);
```

### 3.2 Window Operations

#### Tumbling Window (Non-overlapping)

```
Time:    |--1min--|--1min--|--1min--|
Window:  [  W1   ][  W2   ][  W3   ]
Events:   ●●●      ●●       ●●●●
```

```sql
SELECT
    symbol,
    window_start,
    COUNT(*) AS trade_count
FROM TABLE(
    TUMBLE(TABLE trades, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end
```

#### Sliding Window (Overlapping)

```
Time:       |----2min----|
            |----2min----|
            |----2min----|
Slide:      |--1min--|

Window 1:   [────────────]
Window 2:       [────────────]
Window 3:           [────────────]
```

```sql
SELECT ...
FROM TABLE(
    HOP(TABLE trades, DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE,   -- slide
        INTERVAL '2' MINUTES)  -- size
)
```

### 3.3 Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| COUNT(*) | Number of rows | COUNT(*) |
| SUM(col) | Sum of values | SUM(qty) |
| AVG(col) | Average | AVG(price) |
| MIN(col) | Minimum | MIN(price) |
| MAX(col) | Maximum | MAX(price) |
| FIRST_VALUE(col) | First value | FIRST_VALUE(price) |
| LAST_VALUE(col) | Last value | LAST_VALUE(price) |

### 3.4 Hands-on Experiment

```bash
docker exec -it flink-jobmanager python /tmp/03_flink_window.py
```

---

## Chapter 4: Apache Iceberg

### 4.1 The Problem Iceberg Solves

**Traditional approach** (just Parquet files):
```
s3://bucket/trades/
├── part-00000.parquet
├── part-00001.parquet
└── part-00002.parquet

Problems:
❌ No ACID transactions
❌ No schema evolution
❌ Must scan all files
❌ No time travel
❌ Concurrent writes corrupt data
```

**Iceberg solution**:
```
s3://bucket/trades/
├── metadata/
│   ├── v1.metadata.json  (snapshot 1)
│   ├── v2.metadata.json  (snapshot 2) ← current
│   ├── snap-xxx.avro
│   └── manifest-xxx.avro
└── data/
    ├── 00000-xxx.parquet
    └── 00001-xxx.parquet

Benefits:
✅ ACID transactions
✅ Schema evolution
✅ Efficient queries (file pruning)
✅ Time travel
✅ Safe concurrent access
```

### 4.2 Iceberg Architecture

```
                    ┌──────────────────┐
                    │     CATALOG      │
                    │ (tracks tables)  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │    METADATA      │
                    │ (JSON + Avro)    │
                    │                  │
                    │ - Schema         │
                    │ - Partitions     │
                    │ - Snapshots      │
                    │ - File lists     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │      DATA        │
                    │ (Parquet/ORC)    │
                    │                  │
                    │ Actual data      │
                    │ files (immutable)│
                    └──────────────────┘
```

### 4.3 Snapshots and Time Travel

Every write creates a new **snapshot**:

```
Snapshot 1 (Initial)     Snapshot 2 (+data)     Snapshot 3 (Update)
┌──────────────┐        ┌──────────────┐       ┌──────────────┐
│   Files: A   │        │ Files: A, B  │       │ Files: A, B' │
└──────────────┘        └──────────────┘       └──────────────┘
        │                       │                      │
        └───────────────────────┴──────────────────────┘
                                │
                    Query any point in time!
```

**Time travel queries**:
```sql
-- Query specific snapshot
SELECT * FROM trades VERSION AS OF 123456789;

-- Query specific time
SELECT * FROM trades TIMESTAMP AS OF '2024-01-15 10:00:00';
```

### 4.4 Hands-on Experiment

```bash
docker exec -it flink-jobmanager python /tmp/04_iceberg_basics.py
```

---

## Chapter 5: Complete Pipeline

### 5.1 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                                                               │
│   Producer          Kafka           Flink          Iceberg   │
│  (Python)          (Broker)        (Job)          (Tables)   │
│                                                               │
│  ┌───────┐      ┌───────────┐    ┌────────┐    ┌──────────┐ │
│  │Generate├────►│  trades   ├────►│ Read   │    │raw_trades│ │
│  │ trades │      │  topic    │    │ Parse  ├───►│(all data)│ │
│  └───────┘      └───────────┘    │ Window │    └──────────┘ │
│                                   │ Agg    │                  │
│                                   │        │    ┌──────────┐ │
│                                   │        ├───►│  agg_1m  │ │
│                                   └────────┘    │ (OHLCV)  │ │
│                                                  └──────────┘ │
│                                                       │       │
│                                                       ▼       │
│                                                 ┌──────────┐ │
│                                                 │   MinIO   │ │
│                                                 │ (Storage) │ │
│                                                 └──────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### 5.2 Data Flow

1. **Producer** generates trade events:
   ```json
   {"symbol": "BTCUSDT", "ts": 1705641234567, "price": 42150.50, "qty": 0.15, "side": "BUY"}
   ```

2. **Kafka** stores and distributes events by symbol

3. **Flink** reads and processes:
   - **raw_trades**: All events, append-only
   - **agg_1m**: 1-minute OHLCV aggregations

4. **Iceberg** persists to MinIO as Parquet files

### 5.3 Running the Pipeline

```bash
# 1. Start all services
docker compose up -d

# 2. Download JARs (first time only)
.\scripts\download_jars.ps1  # Windows
# or
bash scripts/download_jars.sh  # Linux/Mac

# 3. Initialize MinIO bucket
bash scripts/init_minio.sh

# 4. Initialize Kafka topic
bash scripts/init_kafka.sh

# 5. Start the producer
cd producer && pip install -r requirements.txt && python producer.py

# 6. Submit the Flink job (new terminal)
docker exec -it flink-jobmanager flink run -py /opt/flink/usrlib/jobs/kafka_to_iceberg.py
```

### 5.4 Verification

**Flink Web UI** (http://localhost:8081):
- Job status: RUNNING
- Records processed: increasing

**MinIO Console** (http://localhost:9001):
- Browse: warehouse/iceberg/lakehouse/
- Data files: .parquet files growing

**SQL Verification**:
```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```
```sql
-- See sql/verify.sql for queries
SELECT COUNT(*) FROM raw_trades;
SELECT * FROM agg_1m ORDER BY window_start DESC LIMIT 5;
```

### 5.5 Hands-on Experiment

```bash
python experiments/06_full_pipeline.py
```

---

## Summary

| Component | Role | Analogy |
|-----------|------|---------|
| **Kafka** | Message queue | Conveyor belt |
| **Flink** | Stream processing | Assembly line workers |
| **Iceberg** | Table format | Smart filing cabinet |
| **MinIO** | Object storage | Warehouse shelves |

**Learning progression**:
1. Understand Kafka's pub/sub model
2. Learn Flink SQL for transformations
3. Master window aggregations
4. Understand Iceberg's table format
5. Build the complete pipeline

**Next steps**:
- Take the quiz: `docs/QUIZ.md`
- Follow the roadmap: `docs/DEVELOPMENT_ROADMAP.md`
