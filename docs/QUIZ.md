# Mini Lakehouse Lab - Self-Assessment Quiz

Test your understanding of the streaming data lakehouse concepts!

**Instructions**: Answer each question, then check the answers at the bottom.

---

## Section 1: Kafka (5 questions)

### Q1.1: Topic Partitions
If a Kafka topic has 3 partitions and you have 4 consumers in the same consumer group, what happens?

- [ ] A) All 4 consumers share the partitions equally
- [ ] B) 1 consumer will be idle (no partitions assigned)
- [ ] C) The 4th consumer will read from all partitions
- [ ] D) Kafka will automatically create a 4th partition

### Q1.2: Message Ordering
Messages with the same key are sent to a Kafka topic. Which statement is TRUE?

- [ ] A) Messages are ordered across all partitions
- [ ] B) Messages with the same key always go to the same partition
- [ ] C) Message order is random
- [ ] D) Keys have no effect on partition assignment

### Q1.3: Consumer Groups
Two consumer groups (A and B) are consuming from the same topic. What happens?

- [ ] A) They share the messages (each message goes to one group)
- [ ] B) Both groups receive all messages independently
- [ ] C) Only group A receives messages
- [ ] D) Messages are load-balanced between groups

### Q1.4: Data Durability
What makes Kafka durable (doesn't lose messages)?

- [ ] A) Messages are kept only in memory
- [ ] B) Messages are written to disk and can be replicated
- [ ] C) Messages are sent directly to consumers
- [ ] D) Kafka uses blockchain technology

### Q1.5: Producer Keys
Why would you use a message key when producing to Kafka?

- [ ] A) To encrypt the message
- [ ] B) To ensure related messages go to the same partition (preserving order)
- [ ] C) To make messages faster
- [ ] D) Keys are required for all messages

---

## Section 2: Flink (8 questions)

### Q2.1: Stream vs Batch
What is the main difference between streaming and batch processing?

- [ ] A) Streaming is slower than batch
- [ ] B) Batch processes data as it arrives, streaming waits for all data
- [ ] C) Streaming processes data continuously as it arrives
- [ ] D) They are the same thing

### Q2.2: Event Time
What is "event time" in Flink?

- [ ] A) The time when Flink processes the event
- [ ] B) The time when the event actually occurred (embedded in data)
- [ ] C) The current wall clock time
- [ ] D) The time zone setting

### Q2.3: Watermarks
What is the purpose of watermarks in Flink?

- [ ] A) To encrypt data
- [ ] B) To signal progress in event time and handle late data
- [ ] C) To speed up processing
- [ ] D) To compress data

### Q2.4: Tumbling Window
A 1-minute tumbling window processes events. If events arrive at 10:00:30 and 10:01:15, which windows do they belong to?

- [ ] A) Both in the same window
- [ ] B) 10:00:30 → [10:00-10:01), 10:01:15 → [10:01-10:02)
- [ ] C) Both in window [10:00-10:02)
- [ ] D) Windows are created per event

### Q2.5: Sliding Window
What's the difference between tumbling and sliding windows?

- [ ] A) Tumbling windows can overlap, sliding cannot
- [ ] B) Sliding windows can overlap, tumbling cannot
- [ ] C) They are identical
- [ ] D) Sliding windows are faster

### Q2.6: Checkpointing
Why does Flink use checkpointing?

- [ ] A) To make queries faster
- [ ] B) To enable fault tolerance (recover from failures)
- [ ] C) To compress data
- [ ] D) To encrypt state

### Q2.7: Table API
What is the benefit of using Flink SQL over DataStream API?

- [ ] A) SQL is always faster
- [ ] B) SQL provides a higher-level, more accessible interface
- [ ] C) DataStream API is deprecated
- [ ] D) SQL supports more features

### Q2.8: Statement Set
Why use a StatementSet for multiple INSERT statements?

- [ ] A) It's required by Flink
- [ ] B) To execute multiple sinks efficiently, sharing the same source
- [ ] C) To run statements sequentially
- [ ] D) For better error handling

---

## Section 3: Iceberg (5 questions)

### Q3.1: Table Format
What problem does Iceberg solve that raw Parquet files don't?

- [ ] A) Parquet can't store data
- [ ] B) ACID transactions, schema evolution, and time travel
- [ ] C) Parquet is too slow
- [ ] D) Parquet doesn't work with S3

### Q3.2: Snapshots
What is an Iceberg snapshot?

- [ ] A) A compressed backup
- [ ] B) A point-in-time view of the table (version)
- [ ] C) A photo of the data
- [ ] D) A type of partition

### Q3.3: Time Travel
How do you query data from a specific time in Iceberg?

- [ ] A) You can't, Iceberg only shows current data
- [ ] B) `SELECT * FROM table TIMESTAMP AS OF '2024-01-15'`
- [ ] C) Create a new table with old data
- [ ] D) Restore from backup

### Q3.4: Schema Evolution
What can you do with Iceberg schema evolution?

- [ ] A) Add, drop, or rename columns without rewriting all data
- [ ] B) Only add new columns
- [ ] C) Must rewrite all data for any schema change
- [ ] D) Schema changes are not supported

### Q3.5: Hidden Partitioning
What is Iceberg's "hidden partitioning"?

- [ ] A) Partitions are invisible to users
- [ ] B) You don't need to include partition columns in queries; Iceberg handles it
- [ ] C) Partitions are encrypted
- [ ] D) Partitioning is disabled by default

---

## Section 4: Integration (3 questions)

### Q4.1: Data Flow
In our pipeline, what is the correct data flow order?

- [ ] A) Flink → Kafka → Iceberg → MinIO
- [ ] B) Producer → Kafka → Flink → Iceberg (on MinIO)
- [ ] C) MinIO → Iceberg → Kafka → Flink
- [ ] D) Iceberg → Flink → Kafka → Producer

### Q4.2: MinIO Role
What role does MinIO play in our architecture?

- [ ] A) Message queue
- [ ] B) Stream processor
- [ ] C) S3-compatible object storage for Iceberg data files
- [ ] D) Database

### Q4.3: OHLCV Aggregation
In our agg_1m table, what does OHLCV stand for?

- [ ] A) Output, Hash, Link, Volume, Count
- [ ] B) Open, High, Low, Close, Volume (prices)
- [ ] C) Ordered, Hierarchical, Linked, Validated
- [ ] D) Online, High-frequency, Low-latency, Volume

---

## Answers

<details>
<summary>Click to reveal answers</summary>

### Section 1: Kafka

| Question | Answer | Explanation |
|----------|--------|-------------|
| Q1.1 | **B** | With 3 partitions and 4 consumers, one consumer will be idle because each partition is assigned to exactly one consumer in a group. |
| Q1.2 | **B** | Messages with the same key are hashed to the same partition, ensuring order for related messages. |
| Q1.3 | **B** | Consumer groups are independent; each group receives all messages and maintains its own offset. |
| Q1.4 | **B** | Kafka persists messages to disk and supports replication for durability. |
| Q1.5 | **B** | Keys ensure related messages (e.g., same user, same symbol) go to the same partition, preserving order. |

### Section 2: Flink

| Question | Answer | Explanation |
|----------|--------|-------------|
| Q2.1 | **C** | Streaming processes data continuously as it arrives; batch waits for all data. |
| Q2.2 | **B** | Event time is the timestamp when the event occurred, embedded in the data itself. |
| Q2.3 | **B** | Watermarks tell Flink how far event time has progressed, enabling window triggers and handling late data. |
| Q2.4 | **B** | Each event falls into its respective minute window: [10:00-10:01) and [10:01-10:02). |
| Q2.5 | **B** | Sliding windows overlap (e.g., 5-min window every 1-min); tumbling windows don't overlap. |
| Q2.6 | **B** | Checkpoints save state periodically, enabling recovery from failures without data loss. |
| Q2.7 | **B** | SQL provides a familiar, higher-level interface; DataStream offers more flexibility for complex logic. |
| Q2.8 | **B** | StatementSet allows multiple INSERT statements to share the same source, avoiding redundant reads. |

### Section 3: Iceberg

| Question | Answer | Explanation |
|----------|--------|-------------|
| Q3.1 | **B** | Iceberg adds ACID transactions, schema evolution, time travel, and efficient querying over raw files. |
| Q3.2 | **B** | A snapshot is a version of the table, representing its state at a specific point in time. |
| Q3.3 | **B** | Use `TIMESTAMP AS OF` or `VERSION AS OF` to query historical data. |
| Q3.4 | **A** | Iceberg supports adding, dropping, renaming columns, and widening types without full rewrites. |
| Q3.5 | **B** | Users query using normal columns; Iceberg automatically prunes files based on hidden partitions. |

### Section 4: Integration

| Question | Answer | Explanation |
|----------|--------|-------------|
| Q4.1 | **B** | Producer sends to Kafka → Flink reads and processes → Writes to Iceberg tables stored in MinIO. |
| Q4.2 | **C** | MinIO provides S3-compatible object storage where Iceberg stores its data and metadata files. |
| Q4.3 | **B** | OHLCV = Open, High, Low, Close (prices) + Volume, standard metrics in financial data. |

</details>

---

## Scoring

| Score | Level | Recommendation |
|-------|-------|----------------|
| 18-21 | Expert | You've mastered the concepts! |
| 14-17 | Proficient | Good understanding, review weak areas |
| 10-13 | Intermediate | Review the learning path chapters |
| 0-9 | Beginner | Work through all experiments again |

---

## Next Steps

Based on your results:

1. **Low Kafka score**: Re-run `experiments/01_kafka_basics.py`
2. **Low Flink score**: Study `experiments/02_flink_sql_basics.py` and `03_flink_window.py`
3. **Low Iceberg score**: Review `experiments/04_iceberg_basics.py`
4. **Low Integration score**: Walk through `experiments/06_full_pipeline.py`

Good luck! 🎓
