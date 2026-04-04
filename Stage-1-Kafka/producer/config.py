"""
Configuration for Kafka producer
Separating config makes it easy to switch environments
"""
import os
from dotenv import load_dotenv

load_dotenv()

class KafkaConfig:
    """Kafka connection settings"""
    
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'orders.raw')
    
    # Producer settings
    ACKS = 'all'  # Wait for all replicas (most reliable)
    RETRIES = 3   # Retry failed sends
    COMPRESSION_TYPE = 'gzip'  # Reduce network usage
    
    # Performance tuning
    BATCH_SIZE = 16384  # Bytes per batch
    LINGER_MS = 10      # Wait 10ms to batch messages
    
class DataConfig:
    """Mock data generation settings"""
    
    ORDERS_PER_MINUTE = int(os.getenv('ORDERS_PER_MINUTE', 1000))
    SLEEP_SECONDS = 60 / ORDERS_PER_MINUTE  # 0.06 seconds
    
    # Product categories
    CATEGORIES = ['electronics', 'clothing', 'books', 'home', 'sports']
    
    # Price ranges (min, max) per category
    PRICE_RANGES = {
        'electronics': (50, 2000),
        'clothing': (20, 300),
        'books': (10, 50),
        'home': (30, 500),
        'sports': (25, 400)
    }
    
    # Customer tenure distribution (days)
    # Used for fraud detection - new customers more risky
    TENURE_NEW = (1, 30)       # 40% of customers
    TENURE_REGULAR = (31, 365)  # 40% of customers
    TENURE_LOYAL = (365, 1825)  # 20% of customers