#!/usr/bin/env python3
"""
Experiment 03: Flink Window Aggregations

This experiment teaches Flink window concepts:
- Event time vs Processing time
- Watermarks for handling late data
- Tumbling windows (fixed, non-overlapping)
- Sliding windows (overlapping)
- Session windows (activity-based)

Prerequisites:
- Flink is running (docker compose up -d)

Usage:
    docker exec -it flink-jobmanager python /tmp/03_flink_window.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def create_environment():
    """Create table environment with event time support."""
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()

    t_env = TableEnvironment.create(settings)

    # Configure for event time processing
    t_env.get_config().set("table.exec.source.idle-timeout", "1000")

    return t_env


def experiment_1_time_concepts():
    """
    Experiment 3.1: Time Concepts in Flink

    Understanding the difference between event time and processing time.
    """
    print_section("3.1 Time Concepts in Flink")

    print("""
    Flink has two main time concepts:

    1. EVENT TIME (Recommended)
       - Time when the event actually occurred
       - Embedded in the data (e.g., timestamp field)
       - Allows replaying data with correct results
       - Requires watermarks to handle late data

       Example: A sensor reading at 10:00:00 processed at 10:00:05
                Event time = 10:00:00

    2. PROCESSING TIME
       - Time when Flink processes the event
       - Simplest, no watermarks needed
       - Non-deterministic (results vary on replay)
       - Use when latency matters more than accuracy

       Example: A sensor reading at 10:00:00 processed at 10:00:05
                Processing time = 10:00:05

    Recommendation: Use EVENT TIME for most streaming applications
                    for reproducible, correct results.
    """)


def experiment_2_watermarks():
    """
    Experiment 3.2: Watermarks for Late Data

    Watermarks signal progress in event time.
    """
    print_section("3.2 Watermarks for Late Data")

    print("""
    WATERMARKS: How Flink handles out-of-order events

    Problem: Events may arrive out of order
    ┌──────────────────────────────────────────┐
    │ Events arriving:  9:59, 10:01, 10:00     │
    │                   ↑      ↑      ↑        │
    │                   │      │      └─ LATE! │
    │                   │      └─ Normal       │
    │                   └─ Normal              │
    └──────────────────────────────────────────┘

    Solution: Watermarks with allowed lateness

    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND

    This means:
    - Allow events up to 5 seconds late
    - If current watermark is 10:00:05, accept events from 10:00:00+

    Visual Example:
    ┌─────────────────────────────────────────────────────────┐
    │ Time: 10:00:00  10:00:03  10:00:05  10:00:06           │
    │         ↓         ↓         ↓         ↓                │
    │ Watermark: 9:59:55 → 9:59:58 → 10:00:00 → 10:00:01     │
    │         (allows 5 sec late events)                     │
    └─────────────────────────────────────────────────────────┘

    SQL Definition:
    ```sql
    CREATE TABLE events (
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    )
    ```
    """)


def experiment_3_tumbling_window(t_env: TableEnvironment):
    """
    Experiment 3.3: Tumbling Windows

    Fixed-size, non-overlapping windows.
    """
    print_section("3.3 Tumbling Windows")

    print("""
    TUMBLING WINDOW: Fixed size, no overlap

    Example: 1-minute tumbling window
    ┌──────────────────────────────────────────────────────┐
    │     │ Window 1  │ Window 2  │ Window 3  │            │
    │     │ 10:00-01  │ 10:01-02  │ 10:02-03  │            │
    │     ├───────────┼───────────┼───────────┤            │
    │ ─────●──●───●────●────●─●────●───●───●───────► time  │
    │     │  │   │    │    │ │    │   │   │                │
    │     │  3 events │  3 events │ 3 events │             │
    │     │ in window │ in window │ in window│             │
    └──────────────────────────────────────────────────────┘

    Use cases:
    - Per-minute/hour/day aggregations
    - OHLCV (Open-High-Low-Close-Volume) calculations
    - Rate limiting / counting
    """)

    # Create source table
    t_env.execute_sql("""
        CREATE TABLE trade_events (
            symbol STRING,
            price DOUBLE,
            qty DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10',
            'fields.symbol.length' = '3',
            'fields.price.min' = '100',
            'fields.price.max' = '200',
            'fields.qty.min' = '1',
            'fields.qty.max' = '10'
        )
    """)

    # Create tumbling window aggregation
    print("\nSQL for 1-minute tumbling window:")
    print("""
    SELECT
        symbol,
        window_start,
        window_end,
        COUNT(*) AS trade_count,
        SUM(qty) AS total_volume,
        AVG(price) AS avg_price
    FROM TABLE(
        TUMBLE(TABLE trade_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    )
    GROUP BY symbol, window_start, window_end
    """)


def experiment_4_sliding_window():
    """
    Experiment 3.4: Sliding (Hop) Windows

    Overlapping windows with fixed size and slide interval.
    """
    print_section("3.4 Sliding (Hop) Windows")

    print("""
    SLIDING WINDOW: Fixed size with overlap (also called HOP window)

    Example: 2-minute window sliding every 1 minute
    ┌──────────────────────────────────────────────────────┐
    │   Window 1: [10:00 - 10:02)                          │
    │   ├─────────────────────┤                            │
    │                                                       │
    │       Window 2: [10:01 - 10:03)                      │
    │       ├─────────────────────┤                        │
    │                                                       │
    │           Window 3: [10:02 - 10:04)                  │
    │           ├─────────────────────┤                    │
    │                                                       │
    │ ──●──●──●──●──●──●──●──●──●──●──●──► time           │
    │   │     │     │                                      │
    │   10:00 10:01 10:02                                  │
    └──────────────────────────────────────────────────────┘

    Each event may belong to MULTIPLE windows!

    SQL Syntax:
    ```sql
    SELECT window_start, window_end, COUNT(*)
    FROM TABLE(
        HOP(
            TABLE events,
            DESCRIPTOR(event_time),
            INTERVAL '1' MINUTE,    -- slide
            INTERVAL '2' MINUTES    -- size
        )
    )
    GROUP BY window_start, window_end
    ```

    Use cases:
    - Moving averages (e.g., 5-min average every 1 min)
    - Trend detection
    - Smoothing noisy data
    """)


def experiment_5_session_window():
    """
    Experiment 3.5: Session Windows

    Dynamic windows based on activity gaps.
    """
    print_section("3.5 Session Windows")

    print("""
    SESSION WINDOW: Based on activity gaps

    Example: Sessions with 5-minute gap timeout
    ┌──────────────────────────────────────────────────────────┐
    │                                                          │
    │   Session 1          Session 2     Session 3             │
    │   ├───────────┤      ├─────┤       ├───────┤            │
    │   ●──●─●──●           ●──●          ●─●──●               │
    │   │  │ │  │           │  │          │ │  │               │
    │   │  │ │  │ >5min gap │  │  >5min   │ │  │               │
    │   │  │ │  │           │  │   gap    │ │  │               │
    │ ──────────────────────────────────────────────► time     │
    └──────────────────────────────────────────────────────────┘

    Windows close after no events for the gap duration.

    SQL Syntax:
    ```sql
    SELECT
        user_id,
        SESSION_START(event_time, INTERVAL '5' MINUTE) AS session_start,
        SESSION_END(event_time, INTERVAL '5' MINUTE) AS session_end,
        COUNT(*) AS events_in_session
    FROM events
    GROUP BY user_id, SESSION(event_time, INTERVAL '5' MINUTE)
    ```

    Use cases:
    - User session analysis (web/app)
    - Activity detection
    - Conversation grouping
    """)


def experiment_6_window_aggregation_example(t_env: TableEnvironment):
    """
    Experiment 3.6: Complete Window Aggregation Example

    Real-world example: OHLCV calculation.
    """
    print_section("3.6 Complete Example: OHLCV Calculation")

    print("""
    OHLCV (Open-High-Low-Close-Volume) is common in financial data:

    - Open:   First price in the window
    - High:   Maximum price in the window
    - Low:    Minimum price in the window
    - Close:  Last price in the window
    - Volume: Total quantity traded

    SQL Implementation:
    ```sql
    SELECT
        symbol,
        window_start,
        window_end,

        -- OHLC Prices
        FIRST_VALUE(price) AS open_price,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        LAST_VALUE(price) AS close_price,

        -- Volume
        SUM(qty) AS volume,
        COUNT(*) AS trade_count,

        -- Buy/Sell breakdown
        SUM(CASE WHEN side = 'BUY' THEN qty ELSE 0 END) AS buy_volume,
        SUM(CASE WHEN side = 'SELL' THEN qty ELSE 0 END) AS sell_volume

    FROM TABLE(
        TUMBLE(TABLE trades, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    )
    GROUP BY symbol, window_start, window_end
    ```

    Note: FIRST_VALUE and LAST_VALUE need proper ordering for OHLC
    """)


def main():
    """Run all window experiments."""
    print("\n" + "#" * 60)
    print("#  Flink Window Aggregations Experiments")
    print("#" * 60)

    print("""
    This script demonstrates Flink window concepts:

    3.1 Time Concepts      - Event time vs Processing time
    3.2 Watermarks         - Handling late data
    3.3 Tumbling Windows   - Fixed, non-overlapping
    3.4 Sliding Windows    - Fixed, overlapping
    3.5 Session Windows    - Activity-based
    3.6 OHLCV Example      - Real-world application
    """)

    input("Press Enter to start the experiments...")

    experiment_1_time_concepts()
    input("\nPress Enter to continue...")

    experiment_2_watermarks()
    input("\nPress Enter to continue...")

    t_env = create_environment()

    experiment_3_tumbling_window(t_env)
    input("\nPress Enter to continue...")

    experiment_4_sliding_window()
    input("\nPress Enter to continue...")

    experiment_5_session_window()
    input("\nPress Enter to continue...")

    experiment_6_window_aggregation_example(t_env)

    print("\n" + "#" * 60)
    print("#  All window experiments completed!")
    print("#" * 60)


if __name__ == "__main__":
    main()
