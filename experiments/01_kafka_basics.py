#!/usr/bin/env python3
"""
Experiment 01: Kafka Basics

This experiment teaches fundamental Kafka concepts:
- Creating and listing topics
- Sending messages to a topic
- Consuming messages from a topic
- Understanding partitions and consumer groups

Prerequisites:
- Kafka is running (docker compose up -d kafka)
- pip install kafka-python

Usage:
    python experiments/01_kafka_basics.py
"""

import json
import time
from datetime import datetime

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


# Configuration
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_NAME = "experiment_topic"


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def experiment_1_create_topic():
    """
    Experiment 1.1: Create a Kafka Topic

    A topic is like a category or folder where messages are stored.
    Topics can have multiple partitions for parallel processing.
    """
    print_section("1.1 Creating a Kafka Topic")

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    # Create a new topic with 3 partitions
    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=3,
        replication_factor=1
    )

    try:
        admin.create_topics([topic])
        print(f"Topic '{TOPIC_NAME}' created successfully!")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists")

    # List all topics
    topics = admin.list_topics()
    print(f"\nAvailable topics: {topics}")

    admin.close()


def experiment_2_produce_messages():
    """
    Experiment 1.2: Send Messages to Kafka

    A producer sends messages to a topic.
    Messages can have a key (for partitioning) and a value.
    """
    print_section("1.2 Producing Messages")

    # Create a producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Send 10 messages
    for i in range(10):
        # Message key determines which partition the message goes to
        key = f"user_{i % 3}"  # Will distribute across 3 partitions

        # Message value
        value = {
            "id": i,
            "message": f"Hello from message {i}",
            "timestamp": datetime.now().isoformat(),
        }

        # Send message
        future = producer.send(TOPIC_NAME, key=key, value=value)
        record = future.get(timeout=10)

        print(
            f"Sent message {i}: key={key}, "
            f"partition={record.partition}, offset={record.offset}"
        )

    producer.flush()
    producer.close()
    print("\nAll messages sent!")


def experiment_3_consume_messages():
    """
    Experiment 1.3: Consume Messages from Kafka

    A consumer reads messages from a topic.
    Consumers can be part of a consumer group for load balancing.
    """
    print_section("1.3 Consuming Messages")

    # Create a consumer
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="experiment_group",
        auto_offset_reset="earliest",  # Start from the beginning
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,  # Stop after 5 seconds of no messages
    )

    print("Reading messages (will timeout after 5 seconds of no new messages):\n")

    message_count = 0
    for message in consumer:
        print(
            f"Received: partition={message.partition}, "
            f"offset={message.offset}, "
            f"key={message.key}, "
            f"value={message.value}"
        )
        message_count += 1

    consumer.close()
    print(f"\nTotal messages consumed: {message_count}")


def experiment_4_partition_demo():
    """
    Experiment 1.4: Understanding Partitions

    Partitions allow parallel processing.
    Messages with the same key always go to the same partition.
    """
    print_section("1.4 Partition Distribution Demo")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Track partition distribution
    partition_counts = {}

    # Send 30 messages with different keys
    keys = ["alice", "bob", "charlie"]
    for i in range(30):
        key = keys[i % 3]
        value = {"index": i, "user": key}

        future = producer.send(TOPIC_NAME, key=key, value=value)
        record = future.get(timeout=10)

        # Track which partition each key goes to
        if key not in partition_counts:
            partition_counts[key] = {}
        partition = record.partition
        partition_counts[key][partition] = partition_counts[key].get(partition, 0) + 1

    producer.close()

    # Show partition distribution
    print("Partition distribution by key:")
    for key, partitions in partition_counts.items():
        print(f"  Key '{key}': {partitions}")

    print("\nNotice: Same key always goes to the same partition!")


def experiment_5_consumer_groups():
    """
    Experiment 1.5: Consumer Groups

    Multiple consumers in a group share the work.
    Each partition is assigned to exactly one consumer in the group.
    """
    print_section("1.5 Consumer Groups Explanation")

    print("""
    Consumer Group Concept:

    1. A consumer group is a set of consumers that work together.

    2. Each partition is assigned to exactly ONE consumer in the group.

       Topic (3 partitions)     Consumer Group (2 consumers)
       ┌─────────────────┐      ┌─────────────────┐
       │  Partition 0    │ ──── │   Consumer A    │
       │  Partition 1    │ ──── │   (handles P0)  │
       │  Partition 2    │ ──── │   (handles P1)  │
       └─────────────────┘      ├─────────────────┤
                                │   Consumer B    │
                                │   (handles P2)  │
                                └─────────────────┘

    3. If you have MORE consumers than partitions, some consumers
       will be idle (no partitions assigned).

    4. Benefits:
       - Load balancing: work is distributed
       - Fault tolerance: if a consumer fails, partitions are
         reassigned to remaining consumers
       - Scalability: add more consumers to handle more load

    5. Multiple consumer groups can read the same topic independently.
       Each group maintains its own offset (position).
    """)

    # Show current consumer group info
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="experiment_group",
    )
    consumer.subscribe([TOPIC_NAME])

    # Trigger partition assignment
    consumer.poll(timeout_ms=1000)

    print("Current consumer's partition assignment:")
    for tp in consumer.assignment():
        print(f"  - {tp.topic} partition {tp.partition}")

    consumer.close()


def main():
    """Run all Kafka experiments."""
    print("\n" + "#" * 60)
    print("#  Kafka Basics Experiments")
    print("#" * 60)

    print("""
    This script demonstrates fundamental Kafka concepts:

    1.1 Create Topic     - Create a Kafka topic
    1.2 Produce Messages - Send messages to Kafka
    1.3 Consume Messages - Read messages from Kafka
    1.4 Partitions       - Understand how partitioning works
    1.5 Consumer Groups  - Learn about parallel consumption
    """)

    input("Press Enter to start the experiments...")

    experiment_1_create_topic()
    input("\nPress Enter to continue to message production...")

    experiment_2_produce_messages()
    input("\nPress Enter to continue to message consumption...")

    experiment_3_consume_messages()
    input("\nPress Enter to continue to partition demo...")

    experiment_4_partition_demo()
    input("\nPress Enter to continue to consumer groups explanation...")

    experiment_5_consumer_groups()

    print("\n" + "#" * 60)
    print("#  All Kafka experiments completed!")
    print("#" * 60)


if __name__ == "__main__":
    main()
