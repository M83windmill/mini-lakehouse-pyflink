#!/usr/bin/env python3
"""
Experiment 05: MinIO and S3A Protocol

This experiment teaches MinIO and S3-compatible storage concepts:
- What is object storage
- MinIO basics
- S3A protocol for Hadoop ecosystem
- Connecting Flink to MinIO

Prerequisites:
- MinIO is running (docker compose up -d minio)
- pip install minio (for local testing)

Usage:
    python experiments/05_minio_s3a.py
"""

import io
import json
from datetime import datetime

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def experiment_1_object_storage_concepts():
    """
    Experiment 5.1: What is Object Storage?

    Understanding the difference from file systems.
    """
    print_section("5.1 What is Object Storage?")

    print("""
    OBJECT STORAGE vs FILE SYSTEM

    File System (like your hard drive):
    ┌──────────────────────────────────────────────────────┐
    │  /                                                    │
    │  ├── home/                                           │
    │  │   └── user/                                       │
    │  │       └── documents/                              │
    │  │           └── report.pdf                          │
    │  └── var/                                            │
    │      └── log/                                        │
    │          └── app.log                                 │
    │                                                       │
    │  Features:                                           │
    │  - Hierarchical directories                          │
    │  - Can modify files in place                         │
    │  - Good for small files, random access               │
    └──────────────────────────────────────────────────────┘

    Object Storage (like S3/MinIO):
    ┌──────────────────────────────────────────────────────┐
    │  Bucket: my-data-lake                                │
    │  ├── warehouse/iceberg/trades/data/000.parquet      │
    │  ├── warehouse/iceberg/trades/data/001.parquet      │
    │  └── raw/events/2024/01/15/events.json              │
    │                                                       │
    │  Features:                                           │
    │  - Flat namespace (/ in keys is just convention)     │
    │  - Objects are immutable (replace, not modify)       │
    │  - Infinitely scalable                               │
    │  - Great for big data, analytics                     │
    │  - HTTP/REST API access                              │
    └──────────────────────────────────────────────────────┘

    Key concepts:
    - BUCKET: Container for objects (like a top-level folder)
    - OBJECT: A file with a unique key
    - KEY: The "path" to the object (e.g., "data/file.parquet")
    - METADATA: Custom attributes attached to objects
    """)


def experiment_2_minio_intro():
    """
    Experiment 5.2: What is MinIO?

    Understanding MinIO as S3-compatible storage.
    """
    print_section("5.2 What is MinIO?")

    print("""
    MINIO: S3-compatible object storage

    MinIO provides:
    ┌──────────────────────────────────────────────────────┐
    │                                                       │
    │  ┌─────────┐     S3 API      ┌─────────────────┐    │
    │  │ Client  │ ───────────────► │     MinIO       │    │
    │  │ (AWS    │                  │                 │    │
    │  │  SDK)   │ ◄─────────────── │  (Local/Cloud)  │    │
    │  └─────────┘                  └─────────────────┘    │
    │                                                       │
    │  Works with ANY tool that supports S3:               │
    │  - AWS CLI                                           │
    │  - Spark, Flink, Trino                              │
    │  - Iceberg, Delta Lake                              │
    │  - Python boto3, JavaScript AWS SDK                 │
    │                                                       │
    └──────────────────────────────────────────────────────┘

    Why MinIO for development:
    - Free and open source
    - Runs locally (no cloud account needed)
    - 100% S3-compatible API
    - Web console for easy browsing
    - Perfect for testing before production S3

    Access in our setup:
    - S3 API:  http://localhost:9000
    - Console: http://localhost:9001
    - Credentials: minioadmin / minioadmin
    """)


def experiment_3_basic_operations():
    """
    Experiment 5.3: Basic MinIO Operations

    Creating buckets and objects.
    """
    print_section("5.3 Basic MinIO Operations")

    if not MINIO_AVAILABLE:
        print("MinIO Python client not installed. Install with: pip install minio")
        print("\nShowing operations conceptually:")

    print("""
    BASIC OPERATIONS with MinIO/S3:

    1. Create a bucket:
       ```python
       client = Minio("localhost:9000",
                      access_key="minioadmin",
                      secret_key="minioadmin",
                      secure=False)

       client.make_bucket("my-bucket")
       ```

    2. Upload an object:
       ```python
       # From file
       client.fput_object("my-bucket", "data/file.txt", "/local/path/file.txt")

       # From bytes
       data = json.dumps({"key": "value"}).encode()
       client.put_object("my-bucket", "data/data.json",
                         io.BytesIO(data), len(data))
       ```

    3. List objects:
       ```python
       objects = client.list_objects("my-bucket", prefix="data/")
       for obj in objects:
           print(f"{obj.object_name}: {obj.size} bytes")
       ```

    4. Download an object:
       ```python
       # To file
       client.fget_object("my-bucket", "data/file.txt", "/local/path/file.txt")

       # To memory
       response = client.get_object("my-bucket", "data/file.txt")
       data = response.read()
       ```

    5. Delete an object:
       ```python
       client.remove_object("my-bucket", "data/file.txt")
       ```
    """)

    # Run actual operations if MinIO client is available
    if MINIO_AVAILABLE:
        try:
            client = Minio(
                "localhost:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )

            # Create test bucket
            bucket_name = "experiment-bucket"
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"\nCreated bucket: {bucket_name}")
            else:
                print(f"\nBucket already exists: {bucket_name}")

            # Upload a test object
            test_data = json.dumps({
                "experiment": "05_minio_s3a",
                "timestamp": datetime.now().isoformat(),
                "message": "Hello from MinIO experiment!"
            }).encode()

            client.put_object(
                bucket_name,
                "test/hello.json",
                io.BytesIO(test_data),
                len(test_data),
                content_type="application/json"
            )
            print(f"Uploaded object: test/hello.json")

            # List objects
            print("\nObjects in bucket:")
            for obj in client.list_objects(bucket_name, recursive=True):
                print(f"  - {obj.object_name}: {obj.size} bytes")

        except S3Error as e:
            print(f"\nMinIO connection error: {e}")
            print("Make sure MinIO is running: docker compose up -d minio")


def experiment_4_s3a_protocol():
    """
    Experiment 5.4: S3A Protocol

    How Hadoop ecosystem connects to S3/MinIO.
    """
    print_section("5.4 S3A Protocol")

    print("""
    S3A: Hadoop's S3 Client

    Different S3 URL schemes:
    ┌──────────────────────────────────────────────────────┐
    │  s3://     - Native AWS SDK (not for Hadoop)        │
    │  s3n://    - Old Hadoop S3 client (deprecated)      │
    │  s3a://    - Modern Hadoop S3 client (recommended)  │
    └──────────────────────────────────────────────────────┘

    S3A URL format:
    ```
    s3a://bucket-name/path/to/object

    Examples:
    s3a://warehouse/iceberg/trades/data/00000.parquet
    s3a://datalake/raw/events/2024/01/15/events.json
    ```

    Configuration (core-site.xml):
    ```xml
    <!-- MinIO endpoint -->
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>

    <!-- Credentials -->
    <property>
        <name>fs.s3a.access.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>minioadmin</value>
    </property>

    <!-- Path-style access (required for MinIO) -->
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>

    <!-- Disable SSL for local dev -->
    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>false</value>
    </property>
    ```
    """)


def experiment_5_flink_iceberg_minio():
    """
    Experiment 5.5: Flink + Iceberg + MinIO Integration

    How all components work together.
    """
    print_section("5.5 Flink + Iceberg + MinIO Integration")

    print("""
    HOW IT ALL CONNECTS:

    ┌─────────────────────────────────────────────────────────────┐
    │                                                              │
    │  ┌────────────────┐                                         │
    │  │  PyFlink Job   │                                         │
    │  │                │                                         │
    │  │  INSERT INTO   │                                         │
    │  │  iceberg_table │                                         │
    │  └───────┬────────┘                                         │
    │          │                                                   │
    │          ▼                                                   │
    │  ┌────────────────┐     ┌─────────────────────────────────┐ │
    │  │  Iceberg       │     │  Required JARs:                 │ │
    │  │  Catalog       │     │  - iceberg-flink-runtime.jar    │ │
    │  │                │     │  - hadoop-aws.jar               │ │
    │  │  Warehouse:    │     │  - aws-java-sdk-bundle.jar      │ │
    │  │  s3a://wh/ice  │     └─────────────────────────────────┘ │
    │  └───────┬────────┘                                         │
    │          │ S3A Protocol                                     │
    │          │ (uses core-site.xml config)                      │
    │          ▼                                                   │
    │  ┌────────────────┐                                         │
    │  │     MinIO      │                                         │
    │  │                │                                         │
    │  │  warehouse/    │                                         │
    │  │  └── iceberg/  │                                         │
    │  │      └── db/   │                                         │
    │  │          └──table/                                       │
    │  │             ├── metadata/                                │
    │  │             └── data/                                    │
    │  └────────────────┘                                         │
    │                                                              │
    └─────────────────────────────────────────────────────────────┘

    Flink SQL Catalog Definition:
    ```sql
    CREATE CATALOG iceberg_catalog WITH (
        'type' = 'iceberg',
        'catalog-type' = 'hadoop',
        'warehouse' = 's3a://warehouse/iceberg',

        -- S3/MinIO connection (can also be in core-site.xml)
        'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint' = 'http://minio:9000',
        's3.access-key-id' = 'minioadmin',
        's3.secret-access-key' = 'minioadmin',
        's3.path-style-access' = 'true'
    );
    ```
    """)


def experiment_6_verify_minio_data():
    """
    Experiment 5.6: Verify Data in MinIO

    How to browse Iceberg data in MinIO.
    """
    print_section("5.6 Verifying Data in MinIO")

    print("""
    BROWSING ICEBERG DATA IN MINIO:

    1. Open MinIO Console: http://localhost:9001
       Login: minioadmin / minioadmin

    2. Navigate to warehouse bucket → iceberg folder

    3. Directory structure:
       warehouse/
       └── iceberg/
           └── lakehouse/              <- database
               ├── raw_trades/         <- table
               │   ├── metadata/       <- Iceberg metadata
               │   │   ├── v1.metadata.json
               │   │   ├── v2.metadata.json
               │   │   ├── snap-xxx-xxx.avro
               │   │   └── xxx-m0.avro
               │   └── data/           <- Parquet files
               │       ├── 00000-0-xxx.parquet
               │       └── 00001-0-yyy.parquet
               │
               └── agg_1m/             <- aggregation table
                   ├── metadata/
                   └── data/

    4. Understanding the files:
       - v*.metadata.json: Table metadata (schema, snapshots)
       - snap-*.avro: Snapshot manifest lists
       - *-m0.avro: Manifest files (list of data files)
       - *.parquet: Actual data files

    5. Using mc (MinIO client) CLI:
       ```bash
       # List files
       docker exec minio mc ls local/warehouse/iceberg/ --recursive

       # View metadata
       docker exec minio mc cat local/warehouse/iceberg/lakehouse/raw_trades/metadata/v1.metadata.json
       ```
    """)


def main():
    """Run all MinIO/S3A experiments."""
    print("\n" + "#" * 60)
    print("#  MinIO and S3A Protocol Experiments")
    print("#" * 60)

    print("""
    This script demonstrates MinIO and S3A concepts:

    5.1 Object Storage     - File system vs Object storage
    5.2 What is MinIO      - S3-compatible local storage
    5.3 Basic Operations   - CRUD operations
    5.4 S3A Protocol       - Hadoop's S3 client
    5.5 Integration        - Flink + Iceberg + MinIO
    5.6 Verify Data        - Browse Iceberg files in MinIO
    """)

    input("Press Enter to start the experiments...")

    experiment_1_object_storage_concepts()
    input("\nPress Enter to continue...")

    experiment_2_minio_intro()
    input("\nPress Enter to continue...")

    experiment_3_basic_operations()
    input("\nPress Enter to continue...")

    experiment_4_s3a_protocol()
    input("\nPress Enter to continue...")

    experiment_5_flink_iceberg_minio()
    input("\nPress Enter to continue...")

    experiment_6_verify_minio_data()

    print("\n" + "#" * 60)
    print("#  All MinIO/S3A experiments completed!")
    print("#" * 60)


if __name__ == "__main__":
    main()
