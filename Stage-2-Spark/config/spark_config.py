"""
Spark Streaming Configuration
Centralizes all Spark settings for easy management
"""
import os
from dotenv import load_dotenv

load_dotenv()


class KafkaConfig:
    """Kafka connection settings"""
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'orders.raw')
    FRAUD_TOPIC = os.getenv('KAFKA_FRAUD_TOPIC', 'fraud.alerts')
    
    # Consumer settings
    STARTING_OFFSETS = 'latest'  # or 'earliest' for reprocessing
    MAX_OFFSETS_PER_TRIGGER = '1000'  # Limit batch size


class S3Config:
    """MinIO/S3 settings"""
    ENDPOINT = os.getenv('S3_ENDPOINT', 'http://localhost:9000')
    ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'minioadmin')
    SECRET_KEY = os.getenv('S3_SECRET_KEY', 'minioadmin')
    
    BUCKET_PROCESSED = os.getenv('S3_BUCKET_PROCESSED', 'orders-processed')
    BUCKET_RAW = os.getenv('S3_BUCKET_RAW', 'orders-raw')
    
    # S3A path (Hadoop S3 connector format)
    PATH_VALID_ORDERS = f"s3a://{BUCKET_PROCESSED}/valid/"
    PATH_FRAUD_ALERTS = f"s3a://{BUCKET_PROCESSED}/fraud/"


class SparkConfig:
    """Spark application settings"""
    APP_NAME = os.getenv('SPARK_APP_NAME', 'EcommerceStreamingPipeline')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARN')
    
    # Checkpoint locations (for fault tolerance)
    CHECKPOINT_BASE = os.getenv('CHECKPOINT_LOCATION', './checkpoint')
    CHECKPOINT_VALID = f"{CHECKPOINT_BASE}/valid_orders"
    CHECKPOINT_FRAUD = f"{CHECKPOINT_BASE}/fraud_alerts"
    CHECKPOINT_AGGREGATIONS = f"{CHECKPOINT_BASE}/aggregations"
    
    # Performance tuning
    SHUFFLE_PARTITIONS = '10'  # Default is 200 (too much for local)
    
    @staticmethod
    def get_spark_conf():
        """
        Returns Spark configuration dictionary
        """
        return {
            # S3 Configuration
            "spark.hadoop.fs.s3a.endpoint": S3Config.ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": S3Config.ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": S3Config.SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # Performance
            "spark.sql.shuffle.partitions": SparkConfig.SHUFFLE_PARTITIONS,
            "spark.sql.streaming.checkpointLocation": SparkConfig.CHECKPOINT_BASE,
            
            # Kafka
            "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false",
        }


class FraudConfig:
    """Fraud detection rules"""
    AMOUNT_THRESHOLD = float(os.getenv('FRAUD_AMOUNT_THRESHOLD', 1000))
    TENURE_THRESHOLD = int(os.getenv('FRAUD_TENURE_THRESHOLD', 30))


class WindowConfig:
    """Windowing and watermark settings"""
    WINDOW_DURATION = os.getenv('WINDOW_DURATION', '5 minutes')
    WATERMARK_DELAY = os.getenv('WATERMARK_DELAY', '10 minutes')
    TRIGGER_INTERVAL = os.getenv('TRIGGER_INTERVAL', '5 seconds')