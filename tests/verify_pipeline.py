#!/usr/bin/env python3
"""
Pipeline Verification Script

This script automatically verifies that all components of the
Mini Lakehouse Lab are working correctly.

Usage:
    python tests/verify_pipeline.py

Checks:
1. Docker services are running
2. Kafka topic exists and has messages
3. MinIO bucket exists
4. Flink job is running
5. Iceberg tables have data (via MinIO file check)
"""

import json
import subprocess
import sys
import time
from typing import Tuple


# ANSI colors for output
class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_header(text: str):
    """Print a section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}  {text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}")


def print_check(name: str, passed: bool, message: str = ""):
    """Print a check result."""
    if passed:
        status = f"{Colors.GREEN}[PASS]{Colors.RESET}"
    else:
        status = f"{Colors.RED}[FAIL]{Colors.RESET}"

    print(f"  {status} {name}")
    if message:
        print(f"         {Colors.YELLOW}{message}{Colors.RESET}")


def run_command(cmd: list, timeout: int = 30) -> Tuple[bool, str]:
    """Run a command and return (success, output)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def check_docker_running() -> bool:
    """Check if Docker is running."""
    success, _ = run_command(["docker", "info"])
    return success


def check_service_running(service: str) -> bool:
    """Check if a Docker service is running."""
    success, output = run_command(
        ["docker", "compose", "ps", "--format", "json"]
    )
    if not success:
        return False

    try:
        # Parse JSON output (one object per line)
        for line in output.strip().split("\n"):
            if line:
                container = json.loads(line)
                if service in container.get("Name", ""):
                    state = container.get("State", "")
                    return state == "running"
    except json.JSONDecodeError:
        pass

    return False


def check_kafka_topic() -> Tuple[bool, str]:
    """Check if Kafka topic 'trades' exists."""
    success, output = run_command([
        "docker", "exec", "kafka",
        "kafka-topics.sh",
        "--bootstrap-server", "localhost:9092",
        "--list"
    ])

    if not success:
        return False, "Cannot connect to Kafka"

    if "trades" in output:
        return True, ""
    else:
        return False, "Topic 'trades' not found"


def check_kafka_messages() -> Tuple[bool, str]:
    """Check if Kafka topic has messages."""
    success, output = run_command([
        "docker", "exec", "kafka",
        "kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:9092",
        "--topic", "trades",
        "--from-beginning",
        "--max-messages", "1",
        "--timeout-ms", "5000"
    ], timeout=10)

    if success and output and "{" in output:
        return True, ""
    else:
        return False, "No messages in topic (is producer running?)"


def check_minio_bucket() -> Tuple[bool, str]:
    """Check if MinIO bucket exists."""
    success, output = run_command([
        "docker", "exec", "minio",
        "mc", "ls", "local/warehouse"
    ])

    if success:
        return True, ""
    else:
        return False, "Bucket 'warehouse' not found"


def check_minio_iceberg_files() -> Tuple[bool, str]:
    """Check if Iceberg data files exist in MinIO."""
    success, output = run_command([
        "docker", "exec", "minio",
        "mc", "ls", "--recursive", "local/warehouse/iceberg/"
    ])

    if not success:
        return False, "Cannot access MinIO"

    if ".parquet" in output:
        return True, ""
    else:
        return False, "No Parquet files found (pipeline may not have written yet)"


def check_flink_job() -> Tuple[bool, str]:
    """Check if Flink job is running."""
    try:
        import urllib.request

        url = "http://localhost:8081/jobs/overview"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())

            running_jobs = [
                j for j in data.get("jobs", [])
                if j.get("state") == "RUNNING"
            ]

            if running_jobs:
                return True, f"{len(running_jobs)} job(s) running"
            else:
                return False, "No running jobs"
    except Exception as e:
        return False, f"Cannot connect to Flink: {e}"


def check_flink_webui() -> bool:
    """Check if Flink Web UI is accessible."""
    try:
        import urllib.request

        url = "http://localhost:8081/overview"
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False


def check_jars_exist() -> Tuple[bool, str]:
    """Check if required JAR files exist."""
    import os

    jars_dir = os.path.join(os.path.dirname(__file__), "..", "jars")
    required_jars = [
        "iceberg-flink-runtime-1.19-1.7.0.jar",
        "flink-sql-connector-kafka-3.2.0-1.19.jar",
        "hadoop-aws-3.3.4.jar",
        "aws-java-sdk-bundle-1.12.648.jar",
    ]

    missing = []
    for jar in required_jars:
        if not os.path.exists(os.path.join(jars_dir, jar)):
            missing.append(jar)

    if missing:
        return False, f"Missing: {', '.join(missing)}"
    return True, ""


def main():
    """Run all verification checks."""
    print(f"\n{Colors.BOLD}Mini Lakehouse Lab - Pipeline Verification{Colors.RESET}")
    print("=" * 60)

    all_passed = True

    # Check Docker
    print_header("1. Docker Environment")

    docker_ok = check_docker_running()
    print_check("Docker running", docker_ok)
    if not docker_ok:
        print(f"\n{Colors.RED}Docker is not running. Please start Docker first.{Colors.RESET}")
        sys.exit(1)

    # Check JARs
    jars_ok, jars_msg = check_jars_exist()
    print_check("JAR files present", jars_ok, jars_msg)
    if not jars_ok:
        all_passed = False

    # Check services
    print_header("2. Docker Services")

    services = ["kafka", "minio", "flink-jobmanager", "flink-taskmanager"]
    for service in services:
        running = check_service_running(service)
        print_check(f"{service} running", running)
        if not running:
            all_passed = False

    # Check Kafka
    print_header("3. Kafka")

    topic_ok, topic_msg = check_kafka_topic()
    print_check("Topic 'trades' exists", topic_ok, topic_msg)
    if not topic_ok:
        all_passed = False

    msgs_ok, msgs_msg = check_kafka_messages()
    print_check("Topic has messages", msgs_ok, msgs_msg)
    if not msgs_ok:
        all_passed = False

    # Check MinIO
    print_header("4. MinIO")

    bucket_ok, bucket_msg = check_minio_bucket()
    print_check("Bucket 'warehouse' exists", bucket_ok, bucket_msg)
    if not bucket_ok:
        all_passed = False

    files_ok, files_msg = check_minio_iceberg_files()
    print_check("Iceberg data files exist", files_ok, files_msg)
    if not files_ok:
        all_passed = False

    # Check Flink
    print_header("5. Flink")

    webui_ok = check_flink_webui()
    print_check("Web UI accessible (localhost:8081)", webui_ok)
    if not webui_ok:
        all_passed = False

    job_ok, job_msg = check_flink_job()
    print_check("Pipeline job running", job_ok, job_msg)
    if not job_ok:
        all_passed = False

    # Summary
    print_header("Summary")

    if all_passed:
        print(f"\n  {Colors.GREEN}{Colors.BOLD}All checks passed!{Colors.RESET}")
        print(f"\n  The pipeline is running correctly.")
        print(f"  - View Flink UI: http://localhost:8081")
        print(f"  - View MinIO:    http://localhost:9001")
    else:
        print(f"\n  {Colors.RED}{Colors.BOLD}Some checks failed.{Colors.RESET}")
        print(f"\n  Please review the failed checks above and:")
        print(f"  1. Ensure all services are running: docker compose up -d")
        print(f"  2. Download JARs: .\\scripts\\download_jars.ps1")
        print(f"  3. Initialize MinIO: bash scripts/init_minio.sh")
        print(f"  4. Initialize Kafka: bash scripts/init_kafka.sh")
        print(f"  5. Start producer: python producer/producer.py")
        print(f"  6. Submit Flink job (see README.md)")

    print()
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
