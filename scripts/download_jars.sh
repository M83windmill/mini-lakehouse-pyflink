#!/bin/bash
# Shell script to download required JAR files for Flink connectors
# Usage: bash scripts/download_jars.sh

set -e

# Define the jars directory (relative to script location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="$SCRIPT_DIR/../jars"

# Create jars directory if it doesn't exist
mkdir -p "$JARS_DIR"

echo "Downloading JAR files to $JARS_DIR..."
echo ""

# Function to download a JAR file
download_jar() {
    local name=$1
    local url=$2
    local dest="$JARS_DIR/$name"

    if [ -f "$dest" ]; then
        echo "[SKIP] $name already exists"
    else
        echo "[DOWNLOAD] $name..."
        if curl -fsSL -o "$dest" "$url"; then
            echo "[OK] $name downloaded successfully"
        else
            echo "[ERROR] Failed to download $name"
            exit 1
        fi
    fi
}

# Download required JAR files
download_jar "iceberg-flink-runtime-1.19-1.7.0.jar" \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.7.0/iceberg-flink-runtime-1.19-1.7.0.jar"

download_jar "flink-sql-connector-kafka-3.2.0-1.19.jar" \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar"

download_jar "hadoop-aws-3.3.4.jar" \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

download_jar "aws-java-sdk-bundle-1.12.648.jar" \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar"

echo ""
echo "All JAR files downloaded successfully!"
echo ""
echo "Downloaded files:"
ls -la "$JARS_DIR"/*.jar 2>/dev/null || echo "  (no JAR files found)"
