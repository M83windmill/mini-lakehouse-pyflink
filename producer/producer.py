#!/usr/bin/env python3
"""
Trade Event Producer
Continuously produces simulated trade events to Kafka topic 'trades'.

Event Schema:
{
    "symbol": "BTCUSDT",     # Trading pair (BTCUSDT/ETHUSDT/SOLUSDT)
    "ts": 1705641234567,     # Unix timestamp in milliseconds
    "price": 42150.50,       # Trade price
    "qty": 0.15,             # Trade quantity
    "side": "BUY"            # Trade side (BUY/SELL)
}

Usage:
    python producer.py [--bootstrap-servers localhost:9092] [--topic trades] [--rate 5]
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Trading symbols and their base prices
SYMBOLS = {
    "BTCUSDT": {"base_price": 42000, "volatility": 500},
    "ETHUSDT": {"base_price": 2500, "volatility": 50},
    "SOLUSDT": {"base_price": 100, "volatility": 5},
}

# Global flag for graceful shutdown
running = True


def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) for graceful shutdown."""
    global running
    print("\nShutting down producer...")
    running = False


def generate_trade_event() -> dict:
    """Generate a random trade event."""
    symbol = random.choice(list(SYMBOLS.keys()))
    config = SYMBOLS[symbol]

    # Generate price with some randomness around base price
    price = config["base_price"] + random.uniform(
        -config["volatility"], config["volatility"]
    )

    # Generate random quantity
    qty = round(random.uniform(0.01, 10.0), 4)

    # Random side
    side = random.choice(["BUY", "SELL"])

    return {
        "symbol": symbol,
        "ts": int(time.time() * 1000),  # Current time in milliseconds
        "price": round(price, 2),
        "qty": qty,
        "side": side,
    }


def main():
    """Main function to run the producer."""
    global running

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Trade Event Producer for Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="trades",
        help="Kafka topic to send messages to (default: trades)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=5,
        help="Number of messages to send per second (default: 5)",
    )
    args = parser.parse_args()

    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    print(f"Starting Trade Event Producer")
    print(f"  Bootstrap servers: {args.bootstrap_servers}")
    print(f"  Topic: {args.topic}")
    print(f"  Rate: {args.rate} messages/second")
    print(f"  Press Ctrl+C to stop")
    print("-" * 50)

    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # Wait for all replicas
            retries=3,
            retry_backoff_ms=500,
        )
    except KafkaError as e:
        print(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Calculate sleep time between messages
    sleep_time = 1.0 / args.rate

    message_count = 0
    start_time = time.time()

    try:
        while running:
            # Generate and send trade event
            event = generate_trade_event()

            # Use symbol as the key for partitioning
            future = producer.send(args.topic, key=event["symbol"], value=event)

            # Wait for the message to be delivered
            try:
                record_metadata = future.get(timeout=10)
                message_count += 1

                # Print progress
                elapsed = time.time() - start_time
                rate = message_count / elapsed if elapsed > 0 else 0
                timestamp = datetime.fromtimestamp(event["ts"] / 1000).strftime(
                    "%H:%M:%S"
                )
                print(
                    f"[{timestamp}] sent: {event['symbol']} {event['side']:4s} "
                    f"{event['qty']:8.4f} @ {event['price']:10.2f} "
                    f"(partition={record_metadata.partition}, offset={record_metadata.offset}, "
                    f"total={message_count}, rate={rate:.1f}/s)"
                )

            except KafkaError as e:
                print(f"Failed to send message: {e}")

            # Sleep to maintain the desired rate
            time.sleep(sleep_time)

    finally:
        # Flush and close the producer
        print("\nFlushing remaining messages...")
        producer.flush()
        producer.close()
        print(f"Producer stopped. Total messages sent: {message_count}")


if __name__ == "__main__":
    main()
