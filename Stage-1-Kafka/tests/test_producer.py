"""
Unit tests for order producer
Demonstrates testing without running actual Kafka
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Add producer directory to path
producer_path = Path(__file__).parent.parent / 'producer'
sys.path.insert(0, str(producer_path))

from mock_orders_api import OrderGenerator, OrderProducer


class TestOrderGenerator:
    """Test order generation logic"""
    
    def test_order_structure(self):
        """Verify order has all required fields"""
        generator = OrderGenerator()
        order = generator.generate_order()
        
        # Required fields
        assert 'order_id' in order
        assert 'customer_id' in order
        assert 'amount' in order
        assert 'timestamp' in order
        assert 'product_category' in order
        assert 'customer_tenure_days' in order
        
        # Field types
        assert isinstance(order['amount'], float)
        assert order['amount'] > 0
        assert isinstance(order['customer_tenure_days'], int)
        assert order['customer_tenure_days'] >= 1
    
    def test_order_count_increments(self):
        """Verify order counter works"""
        generator = OrderGenerator()
        
        assert generator.order_count == 0
        generator.generate_order()
        assert generator.order_count == 1
        generator.generate_order()
        assert generator.order_count == 2
    
    def test_category_in_allowed_list(self):
        """Verify only valid categories used"""
        from config import DataConfig
        
        generator = OrderGenerator()
        
        # Generate 100 orders and verify all categories valid
        for _ in range(100):
            order = generator.generate_order()
            assert order['product_category'] in DataConfig.CATEGORIES
    
    def test_amount_in_category_range(self):
        """Verify prices match category ranges"""
        from config import DataConfig
        
        generator = OrderGenerator()
        
        for _ in range(50):
            order = generator.generate_order()
            category = order['product_category']
            amount = order['amount']
            
            min_price, max_price = DataConfig.PRICE_RANGES[category]
            assert min_price <= amount <= max_price


class TestOrderProducer:
    """Test Kafka producer (mocked)"""
    
    @patch('mock_orders_api.KafkaProducer')
    def test_producer_initialization(self, mock_kafka):
        """Test producer connects correctly"""
        producer = OrderProducer()
        
        # Verify KafkaProducer was called
        mock_kafka.assert_called_once()
        
        # Verify config was passed
        call_kwargs = mock_kafka.call_args.kwargs
        assert 'bootstrap_servers' in call_kwargs
        assert 'value_serializer' in call_kwargs
    
    @patch('mock_orders_api.KafkaProducer')
    def test_send_order_uses_correct_key(self, mock_kafka):
        """Test customer_id used as partition key"""
        # Setup mock
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_future.get.return_value = Mock(partition=0, offset=1)
        mock_producer_instance.send.return_value = mock_future
        mock_kafka.return_value = mock_producer_instance
        
        producer = OrderProducer()
        
        # Create test order
        order = {
            'order_id': 'test-123',
            'customer_id': 'cust-456',
            'amount': 100.0,
            'product_category': 'electronics',
            'customer_tenure_days': 30,
            'timestamp': '2026-01-01T00:00:00Z'
        }
        
        # Send order
        producer.send_order(order)
        
        # Verify send was called with customer_id as key
        mock_producer_instance.send.assert_called_once()
        call_kwargs = mock_producer_instance.send.call_args.kwargs
        assert call_kwargs['key'] == 'cust-456'
        assert call_kwargs['topic'] == 'orders.raw'

