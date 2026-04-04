"""
Order Consumer - Verification Tool
Reads orders from Kafka topic and displays them

WHY THIS EXISTS:
- Proves producer is working
- Shows message structure
- Debugging tool for pipeline

WHAT IT DOES:
1. Connects to Kafka topic "orders.raw"
2. Reads messages from beginning
3. Deserializes JSON
4. Prints to console with formatting

HOW TO RUN (PowerShell):
python consumer\verify_topic.py
"""

import json
import logging
from typing import Dict, Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderConsumer:
    """Kafka consumer for verifying order flow"""
    
    def __init__(
        self,
        topic: str = 'orders.raw',
        bootstrap_servers: str = 'localhost:9092',
        group_id: str = 'verification-consumer'
    ):
        """
        Initialize consumer
        
        Args:
            topic: Kafka topic to read from
            bootstrap_servers: Kafka connection string
            group_id: Consumer group (for offset tracking)
        """
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                
                # Deserialization
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                
                # Start from beginning (for testing)
                auto_offset_reset='earliest',
                
                # Commit offsets automatically
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
            logger.info(f"✅ Connected to topic '{topic}'")
            
        except KafkaError as e:
            logger.error(f"❌ Failed to connect: {e}")
            raise
    
    def display_order(self, order: Dict[str, Any], partition: int, offset: int) -> None:
        """
        Pretty print order details
        
        Args:
            order: Order dictionary
            partition: Kafka partition number
            offset: Message offset
        """
        print("\n" + "="*60)
        print(f"📦 ORDER RECEIVED")
        print("="*60)
        print(f"Order ID:       {order['order_id']}")
        print(f"Customer:       {order['customer_id']}")
        print(f"Amount:         ${order['amount']:.2f} {order['currency']}")
        print(f"Category:       {order['product_category']}")
        print(f"Tenure:         {order['customer_tenure_days']} days")
        print(f"Timestamp:      {order['timestamp']}")
        print(f"Kafka Position: partition={partition}, offset={offset}")
        print("="*60)
    
    def consume(self, max_messages: int = None):
        """
        Start consuming messages
        
        Args:
            max_messages: Stop after N messages (None = infinite)
        """
        logger.info("🎧 Listening for orders... (Press Ctrl+C to stop)")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                order = message.value
                
                self.display_order(
                    order,
                    partition=message.partition,
                    offset=message.offset
                )
                
                message_count += 1
                
                # Stop if max reached
                if max_messages and message_count >= max_messages:
                    logger.info(f"✅ Reached {max_messages} messages, stopping")
                    break
                
        except KeyboardInterrupt:
            logger.info("\n⏸️  Stopping consumer...")
        finally:
            self.consumer.close()
            logger.info(f"✅ Consumed {message_count} total messages")


def main():
    """Run consumer"""
    consumer = OrderConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()