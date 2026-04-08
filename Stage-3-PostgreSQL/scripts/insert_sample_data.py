"""
Insert sample data into PostgreSQL for testing (no Kafka/Spark needed).
Generates realistic e-commerce orders with fraud detection logic.
"""
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from database_connection import DatabaseConnection
from datetime import datetime, timedelta
import random
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Sample data
CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']

PRODUCTS = {
    'Electronics': [
        ('Laptop', 899.99), ('Smartphone', 699.99), ('Tablet', 399.99),
        ('Headphones', 149.99), ('Smartwatch', 299.99), ('Keyboard', 79.99),
        ('Mouse', 49.99), ('Monitor', 349.99), ('Webcam', 89.99), ('Speaker', 129.99)
    ],
    'Clothing': [
        ('T-Shirt', 29.99), ('Jeans', 59.99), ('Jacket', 129.99),
        ('Sneakers', 89.99), ('Dress', 79.99), ('Sweater', 49.99),
        ('Shorts', 39.99), ('Boots', 149.99), ('Hat', 24.99), ('Socks', 12.99)
    ],
    'Home & Garden': [
        ('Lamp', 49.99), ('Chair', 199.99), ('Table', 299.99),
        ('Plant Pot', 19.99), ('Rug', 89.99), ('Curtains', 59.99),
        ('Pillow', 29.99), ('Blanket', 69.99), ('Vase', 34.99), ('Mirror', 79.99)
    ],
    'Sports': [
        ('Basketball', 29.99), ('Tennis Racket', 89.99), ('Yoga Mat', 39.99),
        ('Dumbbells', 79.99), ('Running Shoes', 119.99), ('Bicycle', 499.99),
        ('Gym Bag', 49.99), ('Water Bottle', 19.99), ('Resistance Bands', 24.99), ('Jump Rope', 14.99)
    ],
    'Books': [
        ('Fiction Novel', 14.99), ('Cookbook', 24.99), ('Biography', 19.99),
        ('Self-Help', 16.99), ('Science Fiction', 18.99), ('Mystery', 15.99),
        ('Fantasy', 17.99), ('History', 22.99), ('Travel Guide', 21.99), ('Art Book', 34.99)
    ]
}

def generate_sample_orders(num_orders=1000):
    """Generate sample order data with realistic patterns."""
    logger.info(f"🎲 Generating {num_orders} sample orders...")
    
    orders = []
    start_date = datetime.now() - timedelta(days=30)
    
    # Generate unique customer IDs (simulate repeat customers)
    num_customers = num_orders // 5  # Average 5 orders per customer
    customer_ids = [f"CUST-{i+1:04d}" for i in range(num_customers)]
    
    for i in range(num_orders):
        order_id = f"ORD-{i+1:06d}"
        customer_id = random.choice(customer_ids)
        
        # Select product
        category = random.choice(CATEGORIES)
        product_name, base_price = random.choice(PRODUCTS[category])
        product_id = f"PROD-{category[:3].upper()}-{random.randint(1, 100):03d}"
        
        # Add price variation (±20%)
        price = round(base_price * random.uniform(0.8, 1.2), 2)
        quantity = random.randint(1, 5)
        amount = round(price * quantity, 2)
        
        # Random timestamp in last 30 days (with realistic patterns)
        days_ago = random.randint(0, 30)
        # Peak hours: 10am-2pm, 6pm-9pm
        hour = random.choices(
            range(24),
            weights=[1]*6 + [3]*4 + [5]*4 + [3]*4 + [5]*3 + [2]*3,
            k=1
        )[0]
        
        order_timestamp = start_date + timedelta(
            days=days_ago,
            hours=hour,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Customer age (days since registration)
        # Newer customers tend to be associated with fraud
        customer_age_days = random.choices(
            [random.randint(1, 29), random.randint(30, 365)],
            weights=[0.15, 0.85],  # 15% new customers
            k=1
        )[0]
        
        # Fraud detection logic (matching Spark logic from Stage 2)
        is_fraud = amount > 1000 and customer_age_days < 30
        fraud_reason = "High amount + New customer" if is_fraud else None
        
        # Partition columns (for compatibility with Spark output)
        year = order_timestamp.year
        month = order_timestamp.month
        day = order_timestamp.day
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'price': price,
            'quantity': quantity,
            'amount': amount,
            'order_timestamp': order_timestamp,
            'customer_age_days': customer_age_days,
            'is_fraud': is_fraud,
            'fraud_reason': fraud_reason,
            'year': year,
            'month': month,
            'day': day
        })
    
    logger.info(f"✅ Generated {len(orders)} sample orders")
    return orders

def insert_orders(orders):
    """Insert orders into raw.orders table with batch processing."""
    logger.info(f"\n📥 Inserting {len(orders)} orders into PostgreSQL...")
    
    insert_sql = """
        INSERT INTO raw.orders (
            order_id, customer_id, product_id, product_name, category,
            price, quantity, amount, order_timestamp, customer_age_days,
            is_fraud, fraud_reason, year, month, day
        ) VALUES (
            %(order_id)s, %(customer_id)s, %(product_id)s, %(product_name)s, %(category)s,
            %(price)s, %(quantity)s, %(amount)s, %(order_timestamp)s, %(customer_age_days)s,
            %(is_fraud)s, %(fraud_reason)s, %(year)s, %(month)s, %(day)s
        )
        ON CONFLICT (order_id) DO NOTHING
    """
    
    try:
        with DatabaseConnection.get_cursor() as cursor:
            # Batch insert for performance
            cursor.executemany(insert_sql, orders)
            inserted_count = cursor.rowcount
        
        logger.info(f"✅ Inserted {inserted_count} orders into raw.orders")
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Error inserting data: {e}")
        raise

def verify_data():
    """Verify data was inserted correctly."""
    logger.info("\n📊 Data Verification:")
    
    queries = [
        ("Total orders", "SELECT COUNT(*) FROM raw.orders"),
        ("Unique customers", "SELECT COUNT(DISTINCT customer_id) FROM raw.orders"),
        ("Fraud orders", "SELECT COUNT(*) FROM raw.orders WHERE is_fraud = TRUE"),
        ("Total revenue", "SELECT ROUND(SUM(amount)::numeric, 2) FROM raw.orders"),
        ("Avg order value", "SELECT ROUND(AVG(amount)::numeric, 2) FROM raw.orders"),
        ("Date range", """
            SELECT 
                MIN(order_timestamp)::date as earliest,
                MAX(order_timestamp)::date as latest
            FROM raw.orders
        """),
    ]
    
    try:
        with DatabaseConnection.get_cursor() as cursor:
            for name, query in queries:
                cursor.execute(query)
                result = cursor.fetchone()
                
                if name == "Date range":
                    logger.info(f"   {name}: {result[0]} to {result[1]}")
                elif name == "Total revenue":
                    logger.info(f"   {name}: ${result[0]:,.2f}")
                elif name == "Avg order value":
                    logger.info(f"   {name}: ${result[0]:,.2f}")
                else:
                    logger.info(f"   {name}: {result[0]:,}")
    
    except Exception as e:
        logger.error(f"❌ Verification error: {e}")

def show_sample_data():
    """Display sample of inserted data."""
    logger.info("\n📋 Sample Data (First 5 Orders):")
    
    query = """
        SELECT 
            order_id,
            category,
            product_name,
            amount,
            is_fraud
        FROM raw.orders
        ORDER BY order_timestamp DESC
        LIMIT 5
    """
    
    try:
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"   {'Order ID':<15} {'Category':<15} {'Product':<20} {'Amount':>10} {'Fraud':>8}")
            logger.info("   " + "-" * 73)
            for row in results:
                fraud_flag = "⚠️ YES" if row[4] else "No"
                logger.info(f"   {row[0]:<15} {row[1]:<15} {row[2]:<20} ${row[3]:>9.2f} {fraud_flag:>8}")
    
    except Exception as e:
        logger.error(f"❌ Error fetching sample data: {e}")

def main():
    """Main execution function."""
    logger.info("="*60)
    logger.info("SAMPLE DATA GENERATOR - Stage 3")
    logger.info("="*60)
    
    try:
        # Generate sample orders (adjust number as needed)
        num_orders = 1000
        orders = generate_sample_orders(num_orders=num_orders)
        
        # Insert into database
        inserted = insert_orders(orders)
        
        if inserted > 0:
            # Verify data
            verify_data()
            
            # Show samples
            show_sample_data()
            
            logger.info("\n" + "="*60)
            logger.info("✅ Sample data insertion complete!")
            logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        DatabaseConnection.close_pool()

if __name__ == "__main__":
    main()