"""
Database Configuration Module
Handles PostgreSQL and S3 (MinIO) connections
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

# Load environment variables
load_dotenv()


class DatabaseConfig:
    """PostgreSQL database configuration"""

    # Load from environment
    HOST = os.getenv("POSTGRES_HOST", "localhost")
    PORT = int(os.getenv("POSTGRES_PORT", 5432))
    USER = os.getenv("POSTGRES_USER", "dataeng")
    PASSWORD = os.getenv("POSTGRES_PASSWORD", "dataeng123")
    DATABASE = os.getenv("POSTGRES_DB", "ecommerce")

    # SQLAlchemy connection URL
    DATABASE_URL = (
        f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )

    # Connection pool settings
    POOL_SIZE = 5
    MAX_OVERFLOW = 10
    POOL_TIMEOUT = 30
    POOL_RECYCLE = 3600

    @classmethod
    def get_engine(cls):
        """Create SQLAlchemy engine with connection pooling"""
        print(
            f"[DB] Connecting to {cls.HOST}:{cls.PORT}/{cls.DATABASE} as {cls.USER}"
        )

        return create_engine(
            cls.DATABASE_URL,
            poolclass=QueuePool,
            pool_size=cls.POOL_SIZE,
            max_overflow=cls.MAX_OVERFLOW,
            pool_timeout=cls.POOL_TIMEOUT,
            pool_recycle=cls.POOL_RECYCLE,
            echo=False,
        )

    @classmethod
    def get_connection_string(cls):
        """Return raw connection string (psycopg2 compatible)"""
        return cls.DATABASE_URL


class S3Config:
    """MinIO / S3 configuration"""

    ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
    BUCKET = os.getenv("S3_BUCKET", "orders-processed")

    VALID_ORDERS_PREFIX = "valid/"

    @classmethod
    def get_s3_path(cls, prefix=""):
        """Return S3 path"""
        return f"s3://{cls.BUCKET}/{prefix}"

    @classmethod
    def get_storage_options(cls):
        """Return storage options for pandas / pyarrow"""
        return {
            "key": cls.ACCESS_KEY,
            "secret": cls.SECRET_KEY,
            "client_kwargs": {
                "endpoint_url": cls.ENDPOINT,
            },
            "use_ssl": False,
        }


class LoaderConfig:
    """Batch loader configuration"""

    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Tables
    RAW_TABLE = "raw.orders"
    STAGING_TABLE = "staging.stg_orders"

    # Load strategy
    LOAD_MODE = "append"

    # Data quality
    SKIP_DUPLICATES = True
    VALIDATE_SCHEMA = True