"""
Batch Loader: Parquet (MinIO) → PostgreSQL
Loads validated order data from S3 into PostgreSQL

Usage:
    python scripts/load_parquet_to_postgres.py

Features:
    - Reads Parquet files from MinIO (S3)
    - Handles partitioned data (year/month/day)
    - Deduplicates records
    - Batch inserts for performance
    - Error handling and logging
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import s3fs
from sqlalchemy import text
from loguru import logger
from datetime import datetime

from config.db_config import DatabaseConfig, S3Config, LoaderConfig


class ParquetToPostgresLoader:
    """Load Parquet files from S3 into PostgreSQL"""
    
    def __init__(self):
        """Initialize loader with database and S3 connections"""
        self.engine = DatabaseConfig.get_engine()
        self.s3_path = S3Config.get_s3_path(S3Config.VALID_ORDERS_PREFIX)
        self.storage_options = S3Config.get_storage_options()
        
        # Configure logging
        logger.remove()
        logger.add(
            sys.stdout,
            level=LoaderConfig.LOG_LEVEL,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>"
        )
        logger.add(
            "logs/loader_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="7 days",
            level="DEBUG"
        )
    
    def check_s3_connection(self):
        """
        Verify S3/MinIO connection and list files
        
        Returns:
            bool: True if connection successful
        """
        try:
            logger.info(f"Checking S3 connection: {self.s3_path}")
            
            fs = s3fs.S3FileSystem(
                key=S3Config.ACCESS_KEY,
                secret=S3Config.SECRET_KEY,
                client_kwargs={'endpoint_url': S3Config.ENDPOINT},
                use_ssl=False
            )
            
            # List files
            files = fs.glob(f"{self.s3_path}**/*.parquet")
            logger.info(f"Found {len(files)} Parquet files in S3")
            
            if len(files) == 0:
                logger.warning("No Parquet files found. Run Stage 2 to generate data.")
                return False
            
            # Show sample files
            for file in files[:5]:
                logger.debug(f"Sample file: s3://{file}")
            
            return True
            
        except Exception as e:
            logger.error(f"S3 connection failed: {e}")
            return False
    
    def check_database_connection(self):
        """
        Verify PostgreSQL connection and schema
        
        Returns:
            bool: True if connection successful
        """
        try:
            logger.info("Checking database connection...")
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                logger.info(f"Connected to: {version}")
                
                # Check schemas exist
                result = conn.execute(text("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name IN ('raw', 'staging', 'analytics');
                """))
                schemas = [row[0] for row in result]
                logger.info(f"Found schemas: {schemas}")
                
                # Check table exists
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'raw' AND table_name = 'orders';
                """))
                tables = [row[0] for row in result]
                
                if 'orders' not in tables:
                    logger.error("Table 'raw.orders' not found!")
                    return False
                
                logger.info("Database schema validated")
                return True
                
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def read_parquet_from_s3(self):
        """
        Read all Parquet files from S3
        
        Returns:
            pd.DataFrame: Combined data from all Parquet files
        """
        try:
            logger.info("Reading Parquet files from S3...")
            
            # Read all Parquet files (handles partitioning automatically)
            df = pd.read_parquet(
                self.s3_path,
                storage_options=self.storage_options,
                engine='pyarrow'
            )
            
            logger.info(f"Loaded {len(df):,} records from S3")
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read Parquet files: {e}")
            raise
    
    def transform_data(self, df):
        """
        Transform DataFrame for PostgreSQL
        
        Args:
            df: Raw DataFrame from Parquet
        
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        logger.info("Transforming data...")
        
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Convert timestamp strings to datetime
        if 'timestamp' in df.columns:
            df['event_time'] = pd.to_datetime(df['timestamp'])
        
        if 'kafka_timestamp' in df.columns:
            df['kafka_timestamp'] = pd.to_datetime(df['kafka_timestamp'])
        
        # Ensure required columns exist
        required_cols = ['order_id', 'customer_id', 'amount', 'event_time']
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            raise ValueError(f"Missing columns: {missing_cols}")
        
        # Add loaded_at timestamp
        df['loaded_at'] = datetime.now()
        
        # Select columns that match PostgreSQL schema
        output_cols = [
            'order_id',
            'customer_id',
            'amount',
            'currency',
            'product_category',
            'customer_tenure_days',
            'customer_email',
            'customer_country',
            'is_fraud',
            'fraud_reason',
            'event_time',
            'kafka_timestamp',
            'loaded_at',
            'year',
            'month',
            'day'
        ]
        
        # Only keep columns that exist
        available_cols = [col for col in output_cols if col in df.columns]
        df = df[available_cols]
        
        logger.info(f"Transformed {len(df):,} records")
        logger.info(f"Output columns: {available_cols}")
        
        return df
    
    def deduplicate_data(self, df):
        """
        Remove duplicate records
        
        Args:
            df: DataFrame with potential duplicates
        
        Returns:
            pd.DataFrame: Deduplicated DataFrame
        """
        if not LoaderConfig.SKIP_DUPLICATES:
            return df
        
        logger.info("Checking for duplicates...")
        
        initial_count = len(df)
        df = df.drop_duplicates(subset=['order_id'], keep='first')
        final_count = len(df)
        
        duplicates_removed = initial_count - final_count
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed:,} duplicate records")
        else:
            logger.info("No duplicates found")
        
        return df
    
    def check_existing_records(self, df):
        """
        Check which records already exist in database
        
        Args:
            df: DataFrame to check
        
        Returns:
            pd.DataFrame: Only new records not in database
        """
        try:
            logger.info("Checking for existing records in database...")
            
            # Get list of order_ids
            order_ids = df['order_id'].tolist()
            
            # Query database for existing IDs
            with self.engine.connect() as conn:
                placeholders = ','.join([f"'{oid}'" for oid in order_ids])
                query = text(f"""
                    SELECT order_id 
                    FROM raw.orders 
                    WHERE order_id IN ({placeholders});
                """)
                
                result = conn.execute(query)
                existing_ids = set(row[0] for row in result)
            
            # Filter out existing records
            new_df = df[~df['order_id'].isin(existing_ids)]
            
            existing_count = len(df) - len(new_df)
            if existing_count > 0:
                logger.info(f"Skipping {existing_count:,} existing records")
                logger.info(f"Loading {len(new_df):,} new records")
            else:
                logger.info("All records are new")
            
            return new_df
            
        except Exception as e:
            logger.warning(f"Could not check existing records: {e}")
            logger.info("Proceeding with all records...")
            return df
    
    def load_to_postgres(self, df):
        """
        Load DataFrame into PostgreSQL
        
        Args:
            df: DataFrame to load
        
        Returns:
            int: Number of records loaded
        """
        if len(df) == 0:
            logger.warning("No records to load")
            return 0
        
        try:
            logger.info(f"Loading {len(df):,} records to PostgreSQL...")
            
            # Load data in batches
            records_loaded = 0
            batch_size = LoaderConfig.BATCH_SIZE
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                
                batch.to_sql(
                    name='orders',
                    schema='raw',
                    con=self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                records_loaded += len(batch)
                logger.info(f"Loaded batch {i//batch_size + 1}: {records_loaded:,}/{len(df):,} records")
            
            logger.success(f"Successfully loaded {records_loaded:,} records to raw.orders")
            return records_loaded
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def verify_load(self):
        """
        Verify data was loaded correctly
        
        Returns:
            dict: Load statistics
        """
        try:
            logger.info("Verifying data load...")
            
            with self.engine.connect() as conn:
                # Total record count
                result = conn.execute(text("SELECT COUNT(*) FROM raw.orders;"))
                total_records = result.fetchone()[0]
                
                # Records by date
                result = conn.execute(text("""
                    SELECT 
                        DATE(event_time) as order_date,
                        COUNT(*) as count
                    FROM raw.orders
                    GROUP BY DATE(event_time)
                    ORDER BY order_date DESC
                    LIMIT 5;
                """))
                by_date = result.fetchall()
                
                # Fraud rate
                result = conn.execute(text("""
                    SELECT 
                        COUNT(CASE WHEN is_fraud THEN 1 END) as fraud_count,
                        COUNT(*) as total_count
                    FROM raw.orders;
                """))
                fraud_stats = result.fetchone()
                fraud_rate = (fraud_stats[0] / fraud_stats[1] * 100) if fraud_stats[1] > 0 else 0
                
                # Latest load time
                result = conn.execute(text("SELECT MAX(loaded_at) FROM raw.orders;"))
                latest_load = result.fetchone()[0]
                
                stats = {
                    'total_records': total_records,
                    'fraud_count': fraud_stats[0],
                    'fraud_rate': fraud_rate,
                    'latest_load': latest_load
                }
                
                logger.success("="*60)
                logger.success("LOAD VERIFICATION")
                logger.success("="*60)
                logger.info(f"Total records in database: {total_records:,}")
                logger.info(f"Fraud records: {fraud_stats[0]:,} ({fraud_rate:.2f}%)")
                logger.info(f"Latest load time: {latest_load}")
                logger.info("\nRecords by date:")
                for date, count in by_date:
                    logger.info(f"  {date}: {count:,} orders")
                logger.success("="*60)
                
                return stats
                
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return {}
    
    def run(self):
        """
        Execute complete load process
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info("="*60)
            logger.info("STARTING BATCH LOAD: Parquet → PostgreSQL")
            logger.info("="*60)
            
            # Step 1: Verify connections
            if not self.check_database_connection():
                logger.error("Database connection failed. Aborting.")
                return False
            
            if not self.check_s3_connection():
                logger.error("S3 connection failed. Aborting.")
                return False
            
            # Step 2: Read data from S3
            df = self.read_parquet_from_s3()
            
            # Step 3: Transform data
            df = self.transform_data(df)
            
            # Step 4: Deduplicate
            df = self.deduplicate_data(df)
            
            # Step 5: Check existing records
            df = self.check_existing_records(df)
            
            # Step 6: Load to PostgreSQL
            records_loaded = self.load_to_postgres(df)
            
            # Step 7: Verify
            self.verify_load()
            
            logger.success("Batch load completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Batch load failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


if __name__ == "__main__":
    loader = ParquetToPostgresLoader()
    success = loader.run()
    sys.exit(0 if success else 1)