#!/bin/bash
# Initialize Kafka topic for trade events
# Usage: bash scripts/init_kafka.sh

set -e

echo "Initializing Kafka topic..."

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null; do
    echo "Kafka not ready yet, waiting..."
    sleep 2
done

# Create the trades topic
echo "Creating 'trades' topic..."
docker exec kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic trades \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Verify topic was created
echo ""
echo "Verifying topic..."
docker exec kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic trades

echo ""
echo "Kafka initialization complete!"
echo "  - Topic 'trades' is ready with 3 partitions"
