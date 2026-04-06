"""
Unit tests for fraud detection logic
Tests business rules without running full streaming job
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, IntegerType
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("FraudDetectionTests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_orders(spark):
    """Create sample order data for testing"""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("customer_tenure_days", IntegerType())
    ])
    
    data = [
        # Valid orders
        ("ord-1", "cust-1", 50.0, 100),      # Low amount, old customer
        ("ord-2", "cust-2", 500.0, 200),     # Medium amount, old customer
        ("ord-3", "cust-3", 100.0, 10),      # Low amount, new customer
        
        # Fraud orders
        ("ord-4", "cust-4", 1500.0, 5),      # High amount, new customer
        ("ord-5", "cust-5", 2000.0, 25),     # High amount, new customer
        ("ord-6", "cust-6", 1001.0, 29),     # Just over threshold
    ]
    
    return spark.createDataFrame(data, schema)


class TestFraudDetection:
    """Test fraud detection business logic"""
    
    def test_fraud_rule_high_amount_new_customer(self, sample_orders):
        """Test: amount > 1000 AND tenure < 30 = fraud"""
        from pyspark.sql.functions import when, lit
        
        # Apply fraud rule
        flagged = sample_orders.withColumn(
            "is_fraud",
            when(
                (col("amount") > 1000) & (col("customer_tenure_days") < 30),
                lit(True)
            ).otherwise(lit(False))
        )
        
        fraud_count = flagged.filter(col("is_fraud") == True).count()
        
        # Should detect 3 fraud orders (ord-4, ord-5, ord-6)
        assert fraud_count == 3
    
    def test_valid_orders_filtered_correctly(self, sample_orders):
        """Test: valid orders don't get flagged"""
        from pyspark.sql.functions import when, lit
        
        flagged = sample_orders.withColumn(
            "is_fraud",
            when(
                (col("amount") > 1000) & (col("customer_tenure_days") < 30),
                lit(True)
            ).otherwise(lit(False))
        )
        
        valid_count = flagged.filter(col("is_fraud") == False).count()
        
        # Should have 3 valid orders (ord-1, ord-2, ord-3)
        assert valid_count == 3
    
    def test_edge_case_exactly_1000(self, spark):
        """Test: amount exactly 1000 should be valid"""
        from pyspark.sql.functions import when, lit
        
        data = [("ord-edge", "cust-edge", 1000.0, 10)]
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("customer_tenure_days", IntegerType())
        ])
        
        df = spark.createDataFrame(data, schema)
        
        flagged = df.withColumn(
            "is_fraud",
            when(
                (col("amount") > 1000) & (col("customer_tenure_days") < 30),
                lit(True)
            ).otherwise(lit(False))
        )
        
        is_fraud = flagged.select("is_fraud").collect()[0][0]
        assert is_fraud == False  # Exactly 1000 is NOT fraud
    
    def test_edge_case_exactly_30_days(self, spark):
        """Test: tenure exactly 30 days should be valid"""
        from pyspark.sql.functions import when, lit
        
        data = [("ord-edge2", "cust-edge2", 1500.0, 30)]
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("customer_tenure_days", IntegerType())
        ])
        
        df = spark.createDataFrame(data, schema)
        
        flagged = df.withColumn(
            "is_fraud",
            when(
                (col("amount") > 1000) & (col("customer_tenure_days") < 30),
                lit(True)
            ).otherwise(lit(False))
        )
        
        is_fraud = flagged.select("is_fraud").collect()[0][0]
        assert is_fraud == False  # Exactly 30 days is NOT fraud


class TestDataValidation:
    """Test data quality validation"""
    
    def test_negative_amounts_rejected(self, spark):
        """Test: negative amounts should be filtered"""
        data = [
            ("ord-1", "cust-1", 100.0, 50),
            ("ord-2", "cust-2", -50.0, 100),  # Invalid
            ("ord-3", "cust-3", 0.0, 200),     # Invalid
        ]
        
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("customer_tenure_days", IntegerType())
        ])
        
        df = spark.createDataFrame(data, schema)
        clean = df.filter(col("amount") > 0)
        
        assert clean.count() == 1  # Only ord-1 is valid
    
    def test_negative_tenure_rejected(self, spark):
        """Test: negative tenure should be filtered"""
        data = [
            ("ord-1", "cust-1", 100.0, 50),
            ("ord-2", "cust-2", 100.0, -10),  # Invalid
        ]
        
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("customer_tenure_days", IntegerType())
        ])
        
        df = spark.createDataFrame(data, schema)
        clean = df.filter(col("customer_tenure_days") >= 0)
        
        assert clean.count() == 1


# Run with: pytest tests\test_fraud_logic.py -v