# CLAUDE.md - Development Guide

This file provides context for Claude Code when working on this project.

## Project Overview

Mini Lakehouse Lab is a learning project demonstrating streaming data lakehouse architecture:
- **Kafka** → Message queue (KRaft mode, no Zookeeper)
- **PyFlink** → Stream processing engine (Flink 1.19)
- **Iceberg** → Table format (v1.7.0)
- **MinIO** → S3-compatible object storage

## Architecture

```
Producer (Python) → Kafka (trades topic) → PyFlink → Iceberg Tables (MinIO)
                                              ├── raw_trades (all events)
                                              └── agg_1m (1-min OHLCV)
```

## Key Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Service orchestration |
| `conf/core-site.xml` | S3A/MinIO configuration |
| `jobs/kafka_to_iceberg.py` | Main PyFlink streaming job |
| `producer/producer.py` | Kafka event generator |
| `sql/verify.sql` | Verification queries |

## Technical Details

### Flink Job Configuration
- Parallelism: 2
- Checkpointing: 60 seconds
- Watermark: 5 seconds lateness allowed
- JAR dependencies mounted to `/opt/flink/lib/extra`

### Iceberg Configuration
- Catalog type: Hadoop (file-based)
- Warehouse: `s3a://warehouse/iceberg`
- Format version: 2 (supports row-level deletes)
- Upsert enabled for aggregation updates

### Kafka Configuration
- KRaft mode (no Zookeeper)
- Topic: `trades` with 3 partitions
- Message format: JSON

## Common Tasks

### Starting the Pipeline
```bash
docker compose up -d
bash scripts/init_minio.sh
bash scripts/init_kafka.sh
python producer/producer.py &
docker exec flink-jobmanager flink run -py /opt/flink/usrlib/jobs/kafka_to_iceberg.py
```

### Debugging

Check Flink logs:
```bash
docker compose logs -f flink-jobmanager
```

Check Kafka messages:
```bash
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trades --from-beginning --max-messages 5
```

Access Flink SQL:
```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

### Modifying the Pipeline

1. Edit `jobs/kafka_to_iceberg.py`
2. Cancel existing job in Flink UI
3. Resubmit: `docker exec flink-jobmanager flink run -py ...`

## Code Conventions

### Python Style
- Use type hints
- Docstrings for all public functions
- Follow PEP 8

### SQL Style
- Keywords in UPPERCASE
- One clause per line for complex queries
- Backticks for column names with special characters

### Documentation
- Markdown for all docs
- ASCII diagrams for architecture
- Code examples with syntax highlighting

## Experiment Files

Each experiment file (`experiments/0X_*.py`) is self-contained and educational:
- Interactive with `input()` prompts
- Extensive inline documentation
- Can run standalone

## Testing

Run verification:
```bash
python tests/verify_pipeline.py
```

Manual verification:
- Check Flink UI: http://localhost:8081
- Check MinIO Console: http://localhost:9001
- Query via SQL client

## Dependencies

### Python (producer)
- kafka-python

### Python (experiments)
- kafka-python
- minio (optional, for 05_minio_s3a.py)

### Flink JARs
- iceberg-flink-runtime-1.19-1.7.0.jar
- flink-sql-connector-kafka-3.2.0-1.19.jar
- hadoop-aws-3.3.4.jar
- aws-java-sdk-bundle-1.12.648.jar

## Known Issues

1. **First checkpoint delay**: Iceberg tables may not show data until first checkpoint completes (~60s)
2. **MinIO alias**: Scripts use `docker exec minio mc` which requires the `local` alias to be set
3. **Windows paths**: Some bash scripts may need adjustment for Windows Git Bash

## Future Improvements

- [ ] Add Flink CDC for database ingestion
- [ ] Add Trino/Spark for batch queries
- [ ] Add monitoring with Prometheus/Grafana
- [ ] Add schema registry for Kafka
- [ ] Add partition pruning demos
