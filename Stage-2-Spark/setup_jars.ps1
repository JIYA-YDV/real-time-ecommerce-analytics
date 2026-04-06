# Stage 2 - Download Required JAR Files
# Run this script to set up Spark dependencies
# Usage: .\setup_jars.ps1

Write-Host "Downloading Spark/Kafka/S3 JAR dependencies..." -ForegroundColor Cyan

$jarsDir = "jars"
if (-not (Test-Path $jarsDir)) {
    New-Item -ItemType Directory -Path $jarsDir | Out-Null
}

# JAR files configuration
$jars = @(
    @{
        Name = "hadoop-aws-3.3.4.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    },
    @{
        Name = "aws-java-sdk-bundle-1.12.262.jar"
        Url = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    },
    @{
        Name = "spark-sql-kafka-0-10_2.12-3.5.0.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar"
    },
    @{
        Name = "kafka-clients-3.5.0.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar"
    },
    @{
        Name = "commons-pool2-2.11.1.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
    },
    @{
        Name = "spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
        Url = "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
    }
)

foreach ($jar in $jars) {
    $filePath = Join-Path $jarsDir $jar.Name
    
    if (Test-Path $filePath) {
        Write-Host "[SKIP] $($jar.Name) already exists" -ForegroundColor Yellow
    } else {
        Write-Host "[DOWNLOAD] $($jar.Name)..." -ForegroundColor Green
        try {
            Invoke-WebRequest -Uri $jar.Url -OutFile $filePath -UseBasicParsing
            Write-Host "[OK] Downloaded $($jar.Name)" -ForegroundColor Green
        } catch {
            Write-Host "[ERROR] Failed to download $($jar.Name): $_" -ForegroundColor Red
        }
    }
}

Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Get-ChildItem $jarsDir -Filter *.jar | Format-Table Name, @{Label="Size(MB)";Expression={[math]::Round($_.Length/1MB, 2)}}

Write-Host "`nSetup complete! You can now run Spark jobs." -ForegroundColor Green