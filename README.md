# Mini Lakehouse Lab

A hands-on learning project for understanding streaming data lakehouse architecture with **Kafka**, **PyFlink**, **Iceberg**, and **MinIO**.

```
Producer → Kafka → PyFlink → Iceberg (MinIO)
```

## What You'll Learn

| Component | Role | Analogy |
|-----------|------|---------|
| **Kafka** | Message Queue | Conveyor belt |
| **PyFlink** | Stream Processing | Assembly line |
| **Iceberg** | Table Format | Smart filing cabinet |
| **MinIO** | Object Storage | Warehouse shelves |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- ~4GB RAM available

### 1. Download JAR Files

```powershell
# Windows
.\scripts\download_jars.ps1

# Linux/Mac
bash scripts/download_jars.sh
```

### 2. Start Services

```bash
docker compose up -d
```

Verify services:
- Flink UI: http://localhost:8081
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

### 3. Initialize Infrastructure

```bash
# Create MinIO bucket
bash scripts/init_minio.sh

# Create Kafka topic
bash scripts/init_kafka.sh
```

### 4. Start Data Producer

```bash
cd producer
pip install -r requirements.txt
python producer.py
```

### 5. Submit Flink Job

```bash
docker exec -it flink-jobmanager flink run \
  -py /opt/flink/usrlib/jobs/kafka_to_iceberg.py
```

### 6. Verify Pipeline

```bash
# Automated verification
python tests/verify_pipeline.py

# Manual SQL verification
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

```sql
-- In SQL Client, first configure JARs (see sql/verify.sql)
SELECT COUNT(*) FROM raw_trades;
SELECT * FROM agg_1m ORDER BY window_start DESC LIMIT 5;
```

## Project Structure

```
mini-lakehouse-pyflink/
├── README.md                 # This file
├── CLAUDE.md                 # Development guide
├── docker-compose.yml        # Service orchestration
│
├── docs/                     # Learning materials
│   ├── LEARNING_PATH.md      # Core concepts
│   ├── QUIZ.md               # Self-assessment
│   └── DEVELOPMENT_ROADMAP.md
│
├── experiments/              # Step-by-step tutorials
│   ├── 01_kafka_basics.py
│   ├── 02_flink_sql_basics.py
│   ├── 03_flink_window.py
│   ├── 04_iceberg_basics.py
│   ├── 05_minio_s3a.py
│   └── 06_full_pipeline.py
│
├── jars/                     # Flink connector JARs
├── conf/core-site.xml        # S3A configuration
├── producer/                 # Kafka producer
├── jobs/                     # PyFlink jobs
├── scripts/                  # Init scripts
├── sql/                      # Verification SQL
└── tests/                    # Verification scripts
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│   ┌──────────┐      ┌───────────┐      ┌────────────────────┐  │
│   │ Producer │ ───► │   Kafka   │ ───► │      PyFlink       │  │
│   │ (Python) │      │ (trades)  │      │ (kafka_to_iceberg) │  │
│   └──────────┘      └───────────┘      └─────────┬──────────┘  │
│                                                   │              │
│                                        ┌──────────┴──────────┐  │
│                                        │                     │  │
│                                        ▼                     ▼  │
│                              ┌──────────────┐      ┌───────────┐│
│                              │  raw_trades  │      │  agg_1m   ││
│                              │  (all data)  │      │ (OHLCV)   ││
│                              └──────┬───────┘      └─────┬─────┘│
│                                     │                    │      │
│                                     └────────┬───────────┘      │
│                                              ▼                  │
│                                     ┌────────────────┐          │
│                                     │     MinIO      │          │
│                                     │ (S3 Storage)   │          │
│                                     └────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

## Data Schema

### Trade Events (Kafka)

```json
{
  "symbol": "BTCUSDT",
  "ts": 1705641234567,
  "price": 42150.50,
  "qty": 0.15,
  "side": "BUY"
}
```

### Raw Trades Table (Iceberg)

| Column | Type | Description |
|--------|------|-------------|
| symbol | STRING | Trading pair |
| ts | BIGINT | Timestamp (ms) |
| price | DOUBLE | Trade price |
| qty | DOUBLE | Trade quantity |
| side | STRING | BUY/SELL |
| event_time | TIMESTAMP | Parsed timestamp |

### Aggregation Table (agg_1m)

| Column | Type | Description |
|--------|------|-------------|
| symbol | STRING | Trading pair |
| window_start | TIMESTAMP | Window start time |
| window_end | TIMESTAMP | Window end time |
| open_price | DOUBLE | First price |
| high_price | DOUBLE | Max price |
| low_price | DOUBLE | Min price |
| close_price | DOUBLE | Last price |
| total_qty | DOUBLE | Total volume |
| trade_count | BIGINT | Number of trades |
| buy_qty | DOUBLE | Buy volume |
| sell_qty | DOUBLE | Sell volume |

## Learning Path

1. **Start here**: Read `docs/LEARNING_PATH.md`
2. **Hands-on**: Work through `experiments/01-06`
3. **Test yourself**: Complete `docs/QUIZ.md`
4. **Track progress**: Use `docs/DEVELOPMENT_ROADMAP.md`

## Component Versions

| Component | Version |
|-----------|---------|
| Flink | 1.19.x |
| Iceberg | 1.7.0 |
| Kafka | 3.x (KRaft) |
| MinIO | latest |
| Python | 3.10+ |

## Commands Reference

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# View logs
docker compose logs -f kafka
docker compose logs -f flink-jobmanager

# Access Flink SQL Client
docker exec -it flink-jobmanager ./bin/sql-client.sh

# Cancel Flink job
docker exec -it flink-jobmanager flink cancel <job-id>

# List Kafka topics
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume from Kafka
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trades --from-beginning --max-messages 10
```

## Troubleshooting

### Services won't start
```bash
# Check Docker resources (need ~4GB RAM)
docker system df
docker compose logs
```

### No data in Iceberg tables
1. Check producer is running
2. Check Kafka has messages
3. Check Flink job is RUNNING in UI
4. Wait a few minutes for checkpoints

### JAR loading errors
```bash
# Ensure JARs are downloaded
ls jars/*.jar

# Re-download if needed
.\scripts\download_jars.ps1
```

## License

MIT License - Feel free to use for learning!
