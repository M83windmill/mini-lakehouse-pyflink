#!/bin/bash
# Initialize MinIO bucket for Iceberg warehouse
# Usage: bash scripts/init_minio.sh

set -e

echo "Initializing MinIO bucket..."

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
until docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null; do
    echo "MinIO not ready yet, waiting..."
    sleep 2
done

# Create the warehouse bucket
echo "Creating 'warehouse' bucket..."
docker exec minio mc mb local/warehouse --ignore-existing

# Verify bucket was created
echo ""
echo "Verifying bucket..."
docker exec minio mc ls local/

echo ""
echo "MinIO initialization complete!"
echo "  - Bucket 'warehouse' is ready"
echo "  - Access MinIO Console at: http://localhost:9001"
echo "  - Credentials: minioadmin / minioadmin"
