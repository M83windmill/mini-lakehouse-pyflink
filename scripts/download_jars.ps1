# PowerShell script to download required JAR files for Flink connectors
# Usage: .\scripts\download_jars.ps1

$ErrorActionPreference = "Stop"

# Define the jars directory
$JARS_DIR = Join-Path $PSScriptRoot "..\jars"

# Create jars directory if it doesn't exist
if (-not (Test-Path $JARS_DIR)) {
    New-Item -ItemType Directory -Path $JARS_DIR -Force | Out-Null
}

Write-Host "Downloading JAR files to $JARS_DIR..." -ForegroundColor Cyan

# Define JAR files to download
$jars = @(
    @{
        Name = "iceberg-flink-runtime-1.19-1.7.0.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.7.0/iceberg-flink-runtime-1.19-1.7.0.jar"
    },
    @{
        Name = "flink-sql-connector-kafka-3.2.0-1.19.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar"
    },
    @{
        Name = "hadoop-aws-3.3.4.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    },
    @{
        Name = "aws-java-sdk-bundle-1.12.648.jar"
        Url = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar"
    }
)

# Download each JAR file
foreach ($jar in $jars) {
    $destPath = Join-Path $JARS_DIR $jar.Name

    if (Test-Path $destPath) {
        Write-Host "[SKIP] $($jar.Name) already exists" -ForegroundColor Yellow
    } else {
        Write-Host "[DOWNLOAD] $($jar.Name)..." -ForegroundColor Green
        try {
            Invoke-WebRequest -Uri $jar.Url -OutFile $destPath -UseBasicParsing
            Write-Host "[OK] $($jar.Name) downloaded successfully" -ForegroundColor Green
        } catch {
            Write-Host "[ERROR] Failed to download $($jar.Name): $_" -ForegroundColor Red
            exit 1
        }
    }
}

Write-Host ""
Write-Host "All JAR files downloaded successfully!" -ForegroundColor Cyan
Write-Host ""
Write-Host "Downloaded files:" -ForegroundColor Cyan
Get-ChildItem $JARS_DIR -Filter "*.jar" | ForEach-Object {
    Write-Host "  - $($_.Name)" -ForegroundColor White
}
