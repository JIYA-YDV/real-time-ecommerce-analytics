"""
E-Commerce Order Producer
Generates realistic order events and sends to Kafka

WHY THIS EXISTS:
- Simulates a real shopping website creating orders
- In production, this would be your backend API
- Generates 1000 orders/minute (configurable)

WHAT IT DOES:
1. Creates realistic fake orders using Faker library
2. Serializes to JSON
3. Sends to Kafka topic "orders.raw"
4. Logs success/failures

"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Dict, Any
from uuid import uuid4

from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker

from config import KafkaConfig, DataConfig

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker for realistic data
fake = Faker()


class OrderGenerator:
    """Generates realistic e-commerce orders"""
    
    def __init__(self):
        self.order_count = 0
    
    def generate_order(self) -> Dict[str, Any]:
        """
        Create a single order with realistic data
        
        Returns:
            Dict with order details
        """
        # Random category determines price range
        category = random.choice(DataConfig.CATEGORIES)
        min_price, max_price = DataConfig.PRICE_RANGES[category]
        
        # Customer tenure affects fraud risk
        # 40% new, 40% regular, 20% loyal
        tenure_type = random.choices(
            ['new', 'regular', 'loyal'],
            weights=[0.4, 0.4, 0.2]
        )[0]
        
        if tenure_type == 'new':
            tenure_days = random.randint(*DataConfig.TENURE_NEW)
        elif tenure_type == 'regular':
            tenure_days = random.randint(*DataConfig.TENURE_REGULAR)
        else:
            tenure_days = random.randint(*DataConfig.TENURE_LOYAL)
        
        self.order_count += 1
        
        order = {
            # Unique identifiers
            "order_id": f"ord-{uuid4().hex[:8]}",
            "customer_id": f"cust-{uuid4().hex[:8]}",
            
            # Order details
            "amount": round(random.uniform(min_price, max_price), 2),
            "currency": "USD",
            "product_category": category,
            
            # Customer metadata (for fraud detection in Stage 2)
            "customer_tenure_days": tenure_days,
            "customer_email": fake.email(),
            "customer_country": fake.country_code(),
            
            # Timestamps
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "order_created"
        }
        
        return order


class OrderProducer:
    """Kafka producer for order events"""
    
    def __init__(self):
        """Initialize Kafka producer with config"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                
                # Serialization
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                
                # Reliability
                acks=KafkaConfig.ACKS,
                retries=KafkaConfig.RETRIES,
                
                # Performance
                compression_type=KafkaConfig.COMPRESSION_TYPE,
                batch_size=KafkaConfig.BATCH_SIZE,
                linger_ms=KafkaConfig.LINGER_MS
            )
            logger.info(f"✅ Connected to Kafka at {KafkaConfig.BOOTSTRAP_SERVERS}")
        except KafkaError as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def send_order(self, order: Dict[str, Any]) -> None:
        """
        Send order to Kafka topic
        
        Args:
            order: Order dictionary to send
        """
        try:
            # Use customer_id as key for partitioning
            # Same customer always goes to same partition
            future = self.producer.send(
                topic=KafkaConfig.TOPIC_NAME,
                value=order,
                key=order['customer_id']
            )
            
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"📤 Sent order {order['order_id']}: "
                f"${order['amount']:.2f} - "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            
        except KafkaError as e:
            logger.error(f"❌ Failed to send order {order['order_id']}: {e}")
    
    def close(self):
        """Gracefully close producer"""
        self.producer.flush()  # Send any pending messages
        self.producer.close()
        logger.info("👋 Producer closed")


def main():
    """Main producer loop"""
    logger.info("🚀 Starting Order Producer")
    logger.info(f"📊 Target: {DataConfig.ORDERS_PER_MINUTE} orders/minute")
    logger.info(f"📍 Topic: {KafkaConfig.TOPIC_NAME}")
    
    generator = OrderGenerator()
    producer = OrderProducer()
    
    try:
        while True:
            # Generate and send order
            order = generator.generate_order()
            producer.send_order(order)
            
            # Log progress every 100 orders
            if generator.order_count % 100 == 0:
                logger.info(f"📈 Total orders sent: {generator.order_count}")
            
            # Sleep to maintain desired rate
            time.sleep(DataConfig.SLEEP_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("\n⏸️  Stopping producer...")
    finally:
        producer.close()
        logger.info(f"✅ Sent {generator.order_count} total orders")


if __name__ == "__main__":
    main()