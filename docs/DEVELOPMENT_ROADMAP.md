# Mini Lakehouse Lab - Development Roadmap

This document tracks the learning stages and development milestones for the project.

---

## Overview

```
Stage 1        Stage 2        Stage 3        Stage 4        Stage 5        Stage 6
┌──────┐      ┌──────┐      ┌──────┐      ┌──────┐      ┌──────┐      ┌──────┐
│Setup │ ──► │Kafka │ ──► │Flink │ ──► │Iceberg│ ──► │Full  │ ──► │Test  │
│Env   │      │Learn │      │Learn │      │Learn │      │Pipeline    │Accept│
└──────┘      └──────┘      └──────┘      └──────┘      └──────┘      └──────┘
```

---

## Stage 1: Environment Setup

**Goal**: Get all services running locally

### Tasks

- [ ] **1.1** Clone/create project structure
- [ ] **1.2** Download JAR files
  ```powershell
  .\scripts\download_jars.ps1
  ```
- [ ] **1.3** Start Docker services
  ```bash
  docker compose up -d
  ```
- [ ] **1.4** Verify services are running
  - [ ] Kafka: Check with `docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`
  - [ ] Flink: Open http://localhost:8081
  - [ ] MinIO: Open http://localhost:9001 (login: minioadmin/minioadmin)

### Verification Checklist

- [ ] `docker compose ps` shows all services running
- [ ] Flink Web UI loads
- [ ] MinIO Console loads
- [ ] 4 JAR files exist in `jars/` directory

---

## Stage 2: Kafka Learning

**Goal**: Understand message queuing fundamentals

### Tasks

- [ ] **2.1** Create Kafka topic
  ```bash
  bash scripts/init_kafka.sh
  ```
- [ ] **2.2** Run Kafka basics experiment
  ```bash
  pip install kafka-python
  python experiments/01_kafka_basics.py
  ```
- [ ] **2.3** Start the producer
  ```bash
  cd producer
  pip install -r requirements.txt
  python producer.py
  ```
- [ ] **2.4** Observe messages in Kafka
  ```bash
  docker exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic trades \
    --from-beginning \
    --max-messages 10
  ```

### Learning Objectives

- [ ] Understand what a topic is
- [ ] Understand partitions and why they matter
- [ ] Understand consumer groups
- [ ] Know how keys affect partitioning

### Verification Checklist

- [ ] Can create a topic manually
- [ ] Producer sends messages successfully
- [ ] Can consume messages from topic
- [ ] Can explain partition distribution

---

## Stage 3: Flink Learning

**Goal**: Understand stream processing with Flink SQL

### Tasks

- [ ] **3.1** Copy experiments to Flink container
  ```bash
  docker cp experiments/02_flink_sql_basics.py flink-jobmanager:/tmp/
  docker cp experiments/03_flink_window.py flink-jobmanager:/tmp/
  ```
- [ ] **3.2** Run Flink SQL basics experiment
  ```bash
  docker exec -it flink-jobmanager python /tmp/02_flink_sql_basics.py
  ```
- [ ] **3.3** Run window experiment
  ```bash
  docker exec -it flink-jobmanager python /tmp/03_flink_window.py
  ```
- [ ] **3.4** Explore Flink SQL Client
  ```bash
  docker exec -it flink-jobmanager ./bin/sql-client.sh
  ```

### Learning Objectives

- [ ] Understand DataStream vs Table API
- [ ] Create source and sink tables with DDL
- [ ] Execute basic SELECT queries
- [ ] Understand event time vs processing time
- [ ] Implement tumbling window aggregations
- [ ] Understand watermarks

### Verification Checklist

- [ ] Can define a table with CREATE TABLE
- [ ] Can query data with SELECT
- [ ] Can explain watermarks
- [ ] Can implement 1-minute window aggregation

---

## Stage 4: Iceberg Learning

**Goal**: Understand data lake table format

### Tasks

- [ ] **4.1** Initialize MinIO bucket
  ```bash
  bash scripts/init_minio.sh
  ```
- [ ] **4.2** Run Iceberg basics experiment
  ```bash
  docker cp experiments/04_iceberg_basics.py flink-jobmanager:/tmp/
  docker exec -it flink-jobmanager python /tmp/04_iceberg_basics.py
  ```
- [ ] **4.3** Run MinIO S3A experiment
  ```bash
  pip install minio
  python experiments/05_minio_s3a.py
  ```
- [ ] **4.4** Explore Iceberg files in MinIO Console
  - Navigate to: warehouse/iceberg/

### Learning Objectives

- [ ] Understand why table formats exist
- [ ] Know the three layers: catalog, metadata, data
- [ ] Understand snapshots and versions
- [ ] Know what time travel is
- [ ] Understand schema evolution benefits
- [ ] Understand hidden partitioning

### Verification Checklist

- [ ] Can explain Iceberg vs raw Parquet
- [ ] Can navigate Iceberg files in MinIO
- [ ] Can explain what a snapshot is
- [ ] Can query table metadata

---

## Stage 5: Full Pipeline

**Goal**: Build and run the complete data pipeline

### Tasks

- [ ] **5.1** Review pipeline walkthrough
  ```bash
  python experiments/06_full_pipeline.py
  ```
- [ ] **5.2** Ensure producer is running
  ```bash
  python producer/producer.py
  ```
- [ ] **5.3** Submit Flink job
  ```bash
  docker exec -it flink-jobmanager flink run \
    -py /opt/flink/usrlib/jobs/kafka_to_iceberg.py
  ```
- [ ] **5.4** Monitor in Flink UI
  - Check job status
  - Watch records processed
- [ ] **5.5** Verify data in MinIO
  - Check for new Parquet files

### Learning Objectives

- [ ] Understand end-to-end data flow
- [ ] Know how to submit PyFlink jobs
- [ ] Monitor job health in Flink UI
- [ ] Verify data landed in Iceberg tables

### Verification Checklist

- [ ] Job shows RUNNING in Flink UI
- [ ] Parquet files appear in MinIO
- [ ] raw_trades table has data
- [ ] agg_1m table has data

---

## Stage 6: Acceptance Testing

**Goal**: Verify complete understanding and system functionality

### Tasks

- [ ] **6.1** Run verification SQL
  ```bash
  docker exec -it flink-jobmanager ./bin/sql-client.sh
  # Then run queries from sql/verify.sql
  ```
- [ ] **6.2** Verify raw data
  ```sql
  SELECT COUNT(*) FROM raw_trades;
  SELECT * FROM raw_trades LIMIT 5;
  ```
- [ ] **6.3** Verify aggregations
  ```sql
  SELECT COUNT(*) FROM agg_1m;
  SELECT * FROM agg_1m ORDER BY window_start DESC LIMIT 5;
  ```
- [ ] **6.4** Test time travel (if snapshots exist)
  ```sql
  SELECT * FROM iceberg_catalog.lakehouse.`raw_trades$snapshots`;
  ```
- [ ] **6.5** Take the quiz
  - Complete `docs/QUIZ.md`
  - Score at least 14/21

### Final Verification Checklist

#### Kafka
- [ ] Can create topic
- [ ] Can write messages
- [ ] Can read messages
- [ ] Can explain partitions
- [ ] Can explain consumer groups

#### Flink
- [ ] Can submit PyFlink job
- [ ] Can define source/sink tables
- [ ] Can implement transformations
- [ ] Can implement grouped aggregations
- [ ] Can implement window aggregations

#### Iceberg
- [ ] Can explain data file storage
- [ ] Can explain table organization
- [ ] Can view snapshots
- [ ] Can query raw data
- [ ] Can query aggregated data

#### Integration
- [ ] Kafka receives continuous events
- [ ] Flink job runs continuously
- [ ] MinIO has growing data files
- [ ] Both tables show increasing data

---

## Progress Tracker

| Stage | Status | Date Started | Date Completed | Notes |
|-------|--------|--------------|----------------|-------|
| 1. Environment | ⬜ Not Started | | | |
| 2. Kafka | ⬜ Not Started | | | |
| 3. Flink | ⬜ Not Started | | | |
| 4. Iceberg | ⬜ Not Started | | | |
| 5. Pipeline | ⬜ Not Started | | | |
| 6. Testing | ⬜ Not Started | | | |

**Legend**:
- ⬜ Not Started
- 🔄 In Progress
- ✅ Completed

---

## Troubleshooting Guide

### Common Issues

#### Services won't start
```bash
# Check logs
docker compose logs kafka
docker compose logs flink-jobmanager

# Restart services
docker compose down
docker compose up -d
```

#### JAR files not found
```bash
# Re-download
.\scripts\download_jars.ps1  # Windows
# or
bash scripts/download_jars.sh  # Linux/Mac

# Verify
ls jars/*.jar
```

#### Flink job fails immediately
```bash
# Check job logs in Flink UI
# Common causes:
# - Missing JARs
# - Kafka not ready
# - MinIO bucket doesn't exist
```

#### No data in Iceberg tables
```bash
# Check producer is running
python producer/producer.py

# Check Kafka has messages
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trades --from-beginning --max-messages 5
```

---

## Resources

### Documentation
- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Apache Flink Docs](https://nightlies.apache.org/flink/flink-docs-release-1.19/)
- [Apache Iceberg Docs](https://iceberg.apache.org/docs/latest/)
- [MinIO Docs](https://min.io/docs/minio/linux/index.html)

### This Project
- `docs/LEARNING_PATH.md` - Conceptual learning
- `docs/QUIZ.md` - Self-assessment
- `experiments/` - Hands-on scripts
- `sql/verify.sql` - Verification queries

---

## Completion Certificate

Once all stages are complete and quiz score is 14+:

```
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║         MINI LAKEHOUSE LAB - COMPLETION CERTIFICATE          ║
║                                                              ║
║  This certifies that _____________________ has successfully  ║
║  completed the Mini Lakehouse Lab learning project and       ║
║  demonstrated understanding of:                              ║
║                                                              ║
║  ✓ Apache Kafka message streaming                           ║
║  ✓ Apache Flink stream processing                           ║
║  ✓ Apache Iceberg table format                              ║
║  ✓ MinIO object storage                                     ║
║  ✓ End-to-end streaming data pipeline                       ║
║                                                              ║
║  Date: _______________    Quiz Score: ____/21               ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
```
