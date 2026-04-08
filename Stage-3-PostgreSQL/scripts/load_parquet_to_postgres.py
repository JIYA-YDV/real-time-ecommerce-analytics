"""
Load Parquet files from MinIO to PostgreSQL.
Handles both streaming and batch loading patterns.
"""
import os
import pandas as pd
from minio import Minio
from io import BytesIO
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
from database_connection import DatabaseConnection
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class ParquetLoader:
    """Load Parquet files from MinIO to PostgreSQL."""
    
    def __init__(self):
        """Initialize MinIO client and database connection."""
        self.minio_client = Minio(
            os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
            secure=False
        )
        self.bucket = os.getenv('MINIO_BUCKET', 'orders-processed')
        self.batch_size = int(os.getenv('BATCH_SIZE', 10000))
        
        logger.info(f"Initialized MinIO client: {os.getenv('MINIO_ENDPOINT')}")
        logger.info(f"Target bucket: {self.bucket}")
    
    def list_parquet_files(self, prefix='valid/'):
        """List all Parquet files in MinIO bucket."""
        try:
            objects = self.minio_client.list_objects(
                self.bucket,
                prefix=prefix,
                recursive=True
            )
            
            parquet_files = [
                obj.object_name for obj in objects
                if obj.object_name.endswith('.parquet')
            ]
            
            logger.info(f"Found {len(parquet_files)} Parquet files")
            return parquet_files
            
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    def read_parquet_from_minio(self, object_name):
        """Read Parquet file from MinIO into pandas DataFrame."""
        try:
            # Get object from MinIO
            response = self.minio_client.get_object(self.bucket, object_name)
            
            # Read into BytesIO
            parquet_data = BytesIO(response.read())
            
            # Read with pandas
            df = pd.read_parquet(parquet_data)
            
            # Add metadata
            df['source_file'] = object_name
            df['loaded_at'] = datetime.now()
            
            logger.info(f"Read {len(df)} rows from {object_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading {object_name}: {e}")
            return pd.DataFrame()
        finally:
            response.close()
            response.release_conn()
    
    def load_to_postgres(self, df, table='raw.orders'):
        """Load DataFrame to PostgreSQL table."""
        if df.empty:
            logger.warning("Empty DataFrame, skipping insert")
            return 0
        
        try:
            # Prepare data for insertion
            columns = [
                'order_id', 'customer_id', 'product_id', 'product_name',
                'category', 'price', 'quantity', 'amount', 'order_timestamp',
                'customer_age_days', 'is_fraud', 'fraud_reason',
                'year', 'month', 'day', 'source_file'
            ]
            
            # Ensure all columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            
            # Select only needed columns
            df_insert = df[columns].copy()
            
            # Convert to records
            records = df_insert.to_dict('records')
            
            # Batch insert
            insert_sql = f"""
                INSERT INTO {table} (
                    order_id, customer_id, product_id, product_name, category,
                    price, quantity, amount, order_timestamp, customer_age_days,
                    is_fraud, fraud_reason, year, month, day, source_file
                ) VALUES (
                    %(order_id)s, %(customer_id)s, %(product_id)s, %(product_name)s,
                    %(category)s, %(price)s, %(quantity)s, %(amount)s,
                    %(order_timestamp)s, %(customer_age_days)s, %(is_fraud)s,
                    %(fraud_reason)s, %(year)s, %(month)s, %(day)s, %(source_file)s
                )
                ON CONFLICT (order_id) DO NOTHING
            """
            
            # Insert in batches
            total_inserted = 0
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                
                with DatabaseConnection.get_cursor() as cursor:
                    cursor.executemany(insert_sql, batch)
                    total_inserted += cursor.rowcount
            
            logger.info(f"Inserted {total_inserted} rows into {table}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Error loading to PostgreSQL: {e}")
            raise
    
    def load_all_files(self, prefix='valid/', limit=None):
        """Load all Parquet files from MinIO to PostgreSQL."""
        logger.info("Starting Parquet to PostgreSQL load...")
        
        # Get list of files
        files = self.list_parquet_files(prefix)
        
        if not files:
            logger.warning("No Parquet files found in MinIO")
            return
        
        if limit:
            files = files[:limit]
            logger.info(f"Loading first {limit} files only")
        
        # Statistics
        total_files = len(files)
        total_rows = 0
        failed_files = []
        
        # Process each file with progress bar
        with tqdm(total=total_files, desc="Loading files") as pbar:
            for file_name in files:
                try:
                    # Read Parquet
                    df = self.read_parquet_from_minio(file_name)
                    
                    if not df.empty:
                        # Load to PostgreSQL
                        rows_inserted = self.load_to_postgres(df)
                        total_rows += rows_inserted
                    
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to process {file_name}: {e}")
                    failed_files.append(file_name)
                    pbar.update(1)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("LOAD SUMMARY")
        logger.info("="*60)
        logger.info(f"Total files processed: {total_files}")
        logger.info(f"Total rows inserted: {total_rows:,}")
        logger.info(f"Failed files: {len(failed_files)}")
        if failed_files:
            logger.info(f"Failed file list: {failed_files}")
        logger.info("="*60)
        
        return {
            'total_files': total_files,
            'total_rows': total_rows,
            'failed_files': failed_files
        }
    
    def verify_load(self):
        """Verify data was loaded correctly."""
        queries = {
            'Total rows in raw.orders': "SELECT COUNT(*) FROM raw.orders",
            'Distinct order_ids': "SELECT COUNT(DISTINCT order_id) FROM raw.orders",
            'Fraud orders': "SELECT COUNT(*) FROM raw.orders WHERE is_fraud = TRUE",
            'Date range': "SELECT MIN(order_timestamp), MAX(order_timestamp) FROM raw.orders",
            'Total revenue': "SELECT SUM(amount) FROM raw.orders",
            'Categories': "SELECT category, COUNT(*) FROM raw.orders GROUP BY category",
        }
        
        logger.info("\n" + "="*60)
        logger.info("DATA VERIFICATION")
        logger.info("="*60)
        
        with DatabaseConnection.get_cursor() as cursor:
            for name, query in queries.items():
                cursor.execute(query)
                result = cursor.fetchall()
                logger.info(f"{name}:")
                for row in result:
                    logger.info(f"  {row}")
        
        logger.info("="*60)

def main():
    """Main execution function."""
    loader = ParquetLoader()
    
    # Load all files (or limit for testing)
    result = loader.load_all_files(prefix='valid/', limit=None)
    
    # Verify
    if result and result['total_rows'] > 0:
        loader.verify_load()
    
    # Cleanup
    DatabaseConnection.close_pool()

if __name__ == "__main__":
    main()