"""
Database connection management with connection pooling.
"""
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
import configparser
import os
from pathlib import Path

class DatabaseConnection:
    """Manages PostgreSQL connections with pooling."""
    
    _connection_pool = None
    
    @classmethod
    def initialize_pool(cls, config_file='config/database.ini'):
        """Initialize connection pool on first use."""
        if cls._connection_pool is None:
            config = cls._read_config(config_file)
            
            cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=int(config.get('min_connections', 2)),
                maxconn=int(config.get('max_connections', 10)),
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['user'],
                password=config['password']
            )
            print(f"✅ Connection pool initialized ({config['database']})")
    
    @classmethod
    def _read_config(cls, config_file):
        """Read database configuration from .ini file."""
        config_path = Path(__file__).parent.parent / config_file
        
        if not config_path.exists():
            # Fallback to environment variables
            return {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'port': os.getenv('POSTGRES_PORT', '5432'),
                'database': os.getenv('POSTGRES_DB', 'ecommerce_analytics'),
                'user': os.getenv('POSTGRES_USER', 'ecommerce_user'),
                'password': os.getenv('POSTGRES_PASSWORD', 'ecommerce_pass123'),
                'min_connections': os.getenv('MIN_CONNECTIONS', '2'),
                'max_connections': os.getenv('MAX_CONNECTIONS', '10')
            }
        
        parser = configparser.ConfigParser()
        parser.read(config_path)
        
        return {
            'host': parser.get('postgresql', 'host'),
            'port': parser.get('postgresql', 'port'),
            'database': parser.get('postgresql', 'database'),
            'user': parser.get('postgresql', 'user'),
            'password': parser.get('postgresql', 'password'),
            'min_connections': parser.get('connection_pool', 'min_connections', fallback='2'),
            'max_connections': parser.get('connection_pool', 'max_connections', fallback='10')
        }
    
    @classmethod
    @contextmanager
    def get_connection(cls):
        """Context manager for database connections."""
        cls.initialize_pool()
        
        conn = cls._connection_pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cls._connection_pool.putconn(conn)
    
    @classmethod
    @contextmanager
    def get_cursor(cls):
        """Context manager for database cursors."""
        with cls.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()
    
    @classmethod
    def close_pool(cls):
        """Close all connections in pool."""
        if cls._connection_pool:
            cls._connection_pool.closeall()
            print("🔌 Connection pool closed")

# Test connection
if __name__ == "__main__":
    try:
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"✅ Connected to PostgreSQL:")
            print(f"   {version}")
            
            # Test schemas
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('raw', 'staging', 'analytics')
            """)
            schemas = cursor.fetchall()
            print(f"\n📂 Available schemas: {[s[0] for s in schemas]}")
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        DatabaseConnection.close_pool()