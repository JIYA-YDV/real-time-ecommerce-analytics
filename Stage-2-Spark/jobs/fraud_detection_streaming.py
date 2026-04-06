"""
Real-Time Fraud Detection Streaming Job
========================================

WINDOWS USERS: Run with Python directly, not spark-submit
python jobs\fraud_detection_streaming.py
"""

import os
import sys
from pathlib import Path

# FIX: Windows Python path for PySpark workers
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Add config to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, 
    sum as _sum, count, avg, when, lit,
    to_timestamp, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, IntegerType, TimestampType
)

from config.spark_config import (
    SparkConfig, KafkaConfig, S3Config, 
    FraudConfig, WindowConfig
)


class FraudDetectionPipeline:
    """Main streaming pipeline for fraud detection"""
    
    def __init__(self):
        """Initialize Spark session with S3 configuration"""
        self.spark = self._create_spark_session()
        self.order_schema = self._define_schema()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with all configurations"""
        print("[*] Creating Spark Session...")
        
        # Get absolute path to JARs directory
        jars_dir = Path(__file__).parent.parent / "jars"
        
        # List of JAR files
        jar_files = [
            str(jars_dir / "hadoop-aws-3.3.4.jar"),
            str(jars_dir / "aws-java-sdk-bundle-1.12.262.jar"),
            str(jars_dir / "spark-sql-kafka-0-10_2.12-3.5.0.jar")
        ]
        
        # Check JARs exist
        for jar in jar_files:
            if not Path(jar).exists():
                raise FileNotFoundError(f"JAR not found: {jar}")
        
        # Build Spark session
        builder = SparkSession.builder \
            .appName(SparkConfig.APP_NAME) \
            .master("local[*]") \
            .config("spark.jars", ",".join(jar_files)) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g")
        
        # Apply S3/Kafka configurations
        for key, value in SparkConfig.get_spark_conf().items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(SparkConfig.LOG_LEVEL)
        
        print(f"[OK] Spark Session created: {SparkConfig.APP_NAME}")
        print(f"[INFO] Spark UI: http://localhost:4040")
        
        return spark
    
    def _define_schema(self) -> StructType:
        """Define expected order schema"""
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("customer_tenure_days", IntegerType(), False),
            StructField("customer_email", StringType(), True),
            StructField("customer_country", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("event_type", StringType(), True)
        ])
    
    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        print(f"[*] Reading from Kafka topic: {KafkaConfig.INPUT_TOPIC}")
        
        raw_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS) \
            .option("subscribe", KafkaConfig.INPUT_TOPIC) \
            .option("startingOffsets", KafkaConfig.STARTING_OFFSETS) \
            .option("maxOffsetsPerTrigger", KafkaConfig.MAX_OFFSETS_PER_TRIGGER) \
            .load()
        
        print("[OK] Kafka stream connected")
        return raw_stream
    
    def deserialize_orders(self, raw_stream):
        """Convert Kafka binary messages to structured orders"""
        print("[*] Deserializing JSON messages...")
        
        orders = raw_stream \
            .select(
                from_json(
                    col("value").cast("string"), 
                    self.order_schema
                ).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ) \
            .select("data.*", "kafka_timestamp")
        
        # Convert timestamp string to proper timestamp
        orders = orders.withColumn(
            "event_time", 
            to_timestamp(col("timestamp"))
        )
        
        print("[OK] Messages deserialized")
        return orders
    
    def validate_and_clean(self, orders):
        """Validate data quality and filter bad records"""
        print("[*] Validating and cleaning data...")
        
        clean_orders = orders \
            .filter(col("amount") > 0) \
            .filter(col("customer_tenure_days") >= 0) \
            .filter(col("event_time").isNotNull()) \
            .filter(col("order_id").isNotNull()) \
            .filter(col("customer_id").isNotNull())
        
        print("[OK] Data validated")
        return clean_orders
    
    def detect_fraud(self, orders):
        """Apply fraud detection rules"""
        print(f"[*] Applying fraud detection rules...")
        print(f"    - Amount threshold: ${FraudConfig.AMOUNT_THRESHOLD}")
        print(f"    - Tenure threshold: {FraudConfig.TENURE_THRESHOLD} days")
        
        orders_flagged = orders.withColumn(
            "is_fraud",
            when(
                (col("amount") > FraudConfig.AMOUNT_THRESHOLD) & 
                (col("customer_tenure_days") < FraudConfig.TENURE_THRESHOLD),
                lit(True)
            ).otherwise(lit(False))
        )
        
        orders_flagged = orders_flagged.withColumn(
            "fraud_reason",
            when(
                col("is_fraud") == True,
                lit(f"High value order (>${FraudConfig.AMOUNT_THRESHOLD}) from new customer (<{FraudConfig.TENURE_THRESHOLD} days)")
            ).otherwise(lit(None))
        )
        
        fraud_orders = orders_flagged.filter(col("is_fraud") == True)
        valid_orders = orders_flagged.filter(col("is_fraud") == False)
        
        print("[OK] Fraud detection applied")
        return fraud_orders, valid_orders
    
    def write_valid_to_s3(self, valid_orders):
        """Write valid orders to S3 in Parquet format"""
        print(f"[*] Writing valid orders to S3: {S3Config.PATH_VALID_ORDERS}")
        
        partitioned = valid_orders \
            .withColumn("year", year(col("event_time"))) \
            .withColumn("month", month(col("event_time"))) \
            .withColumn("day", dayofmonth(col("event_time")))
        
        query = partitioned \
            .writeStream \
            .format("parquet") \
            .option("path", S3Config.PATH_VALID_ORDERS) \
            .option("checkpointLocation", SparkConfig.CHECKPOINT_VALID) \
            .partitionBy("year", "month", "day") \
            .trigger(processingTime=WindowConfig.TRIGGER_INTERVAL) \
            .start()
        
        print(f"[OK] Valid orders stream started")
        return query
    
    def write_fraud_to_kafka(self, fraud_orders):
        """Write fraud alerts back to Kafka"""
        print(f"[ALERT] Writing fraud alerts to Kafka: {KafkaConfig.FRAUD_TOPIC}")
        
        fraud_json = fraud_orders.select(
            to_json(struct("*")).alias("value")
        )
        
        query = fraud_json \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS) \
            .option("topic", KafkaConfig.FRAUD_TOPIC) \
            .option("checkpointLocation", SparkConfig.CHECKPOINT_FRAUD) \
            .trigger(processingTime=WindowConfig.TRIGGER_INTERVAL) \
            .start()
        
        print(f"[OK] Fraud alerts stream started")
        return query
    
    def create_windowed_aggregations(self, orders):
        """Create 5-minute windowed aggregations"""
        print(f"[*] Creating windowed aggregations...")
        print(f"    - Window: {WindowConfig.WINDOW_DURATION}")
        print(f"    - Watermark: {WindowConfig.WATERMARK_DELAY}")
        
        windowed = orders \
            .withWatermark("event_time", WindowConfig.WATERMARK_DELAY) \
            .groupBy(
                window(col("event_time"), WindowConfig.WINDOW_DURATION),
                col("product_category")
            ) \
            .agg(
                _sum("amount").alias("total_revenue"),
                count("*").alias("order_count"),
                avg("amount").alias("avg_order_value")
            )
        
        query = windowed \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", SparkConfig.CHECKPOINT_AGGREGATIONS) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        print("[OK] Aggregations stream started")
        return query
    
    def run(self):
        """Main pipeline execution"""
        print("="*60)
        print("   Starting Fraud Detection Pipeline")
        print("="*60)
        
        try:
            raw_stream = self.read_from_kafka()
            orders = self.deserialize_orders(raw_stream)
            clean_orders = self.validate_and_clean(orders)
            fraud_orders, valid_orders = self.detect_fraud(clean_orders)
            
            valid_query = self.write_valid_to_s3(valid_orders)
            fraud_query = self.write_fraud_to_kafka(fraud_orders)
            agg_query = self.create_windowed_aggregations(clean_orders)
            
            print("="*60)
            print("   ALL STREAMS STARTED SUCCESSFULLY")
            print("="*60)
            print("Monitoring:")
            print(f"   - Spark UI: http://localhost:4040")
            print(f"   - MinIO Console: http://localhost:9001")
            print(f"   - Kafka UI: http://localhost:9021")
            print("="*60)
            print("Press Ctrl+C to stop")
            print("="*60)
            
            self.spark.streams.awaitAnyTermination()
            
        except KeyboardInterrupt:
            print("\n[STOP] Stopping streams...")
            for stream in self.spark.streams.active:
                stream.stop()
            print("[OK] Pipeline stopped gracefully")
        except Exception as e:
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.spark.stop()
            print("[*] Spark session closed")


if __name__ == "__main__":
    pipeline = FraudDetectionPipeline()
    pipeline.run()