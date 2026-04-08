"""
Generate sample Parquet files for testing Stage 3
Creates ~1000 orders in MinIO
"""

import pandas as pd
import boto3
from datetime import datetime, timedelta
import random
import uuid
import os

print("="*60)
print("SAMPLE DATA GENERATOR - Stage 3")
print("="*60)

# MinIO configuration
print("\n[1/5] Connecting to MinIO...")
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    use_ssl=False
)

# Verify bucket exists
print("[2/5] Verifying bucket 'orders-processed' exists...")
try:
    s3_client.head_bucket(Bucket='orders-processed')
    print("✅ Bucket found")
except:
    print("⚠️  Bucket not found, creating...")
    s3_client.create_bucket(Bucket='orders-processed')
    print("✅ Bucket created")

# Generate sample data
def generate_orders(n=1000):
    """Generate n sample orders"""
    print(f"\n[3/5] Generating {n} sample orders...")
    
    categories = ['electronics', 'clothing', 'books', 'home', 'sports']
    countries = ['US', 'UK', 'CA', 'AU', 'DE']
    
    orders = []
    base_time = datetime.now()
    
    for i in range(n):
        category = random.choice(categories)
        amount = round(random.uniform(10, 2000), 2)
        tenure = random.randint(1, 500)
        
        # Fraud logic: amount > 1000 AND tenure < 30
        is_fraud = amount > 1000 and tenure < 30
        
        order = {
            'order_id': f'ord-{uuid.uuid4().hex[:8]}',
            'customer_id': f'cust-{uuid.uuid4().hex[:8]}',
            'amount': amount,
            'currency': 'USD',
            'product_category': category,
            'customer_tenure_days': tenure,
            'customer_email': f'user{i}@example.com',
            'customer_country': random.choice(countries),
            'is_fraud': is_fraud,
            'fraud_reason': 'High value order from new customer' if is_fraud else None,
            'timestamp': (base_time - timedelta(hours=random.randint(0, 48))).isoformat(),
            'kafka_timestamp': base_time.isoformat(),
            'event_type': 'order_created'
        }
        orders.append(order)
    
    df = pd.DataFrame(orders)
    
    # Add partition columns
    df['event_time'] = pd.to_datetime(df['timestamp'])
    df['year'] = df['event_time'].dt.year
    df['month'] = df['event_time'].dt.month
    df['day'] = df['event_time'].dt.day
    
    fraud_count = df['is_fraud'].sum()
    print(f"✅ Generated {len(df)} orders")
    print(f"   - Fraud orders: {fraud_count} ({fraud_count/len(df)*100:.1f}%)")
    print(f"   - Categories: {df['product_category'].value_counts().to_dict()}")
    
    return df

# Generate data
df = generate_orders(1000)

# Save to local Parquet file
print("\n[4/5] Saving to Parquet file...")
filename = f"sample_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
df.to_parquet(filename, engine='pyarrow', compression='snappy')
print(f"✅ Saved to {filename}")

# Get file size
file_size = os.path.getsize(filename) / 1024  # KB
print(f"   File size: {file_size:.2f} KB")

# Upload to MinIO
print("\n[5/5] Uploading to MinIO...")
s3_key = f'valid/year=2026/month=4/day=8/{filename}'

try:
    s3_client.upload_file(
        filename,
        'orders-processed',
        s3_key
    )
    print(f"✅ Uploaded to MinIO")
    print(f"   Bucket: orders-processed")
    print(f"   Path: {s3_key}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# Verify upload
print("\nVerifying upload...")
try:
    response = s3_client.list_objects_v2(
        Bucket='orders-processed',
        Prefix='valid/'
    )
    
    if 'Contents' in response:
        files = response['Contents']
        print(f"✅ Found {len(files)} file(s) in MinIO:")
        for f in files:
            print(f"   - {f['Key']} ({f['Size']/1024:.2f} KB)")
    else:
        print("⚠️  No files found in MinIO")
except Exception as e:
    print(f"❌ Verification failed: {e}")

# Clean up local file
os.remove(filename)
print(f"\n🧹 Cleaned up local file: {filename}")

print("\n" + "="*60)
print("✅ SAMPLE DATA GENERATION COMPLETE")
print("="*60)
print("\nNext step: Run the batch loader:")
print("  python scripts\\load_parquet_to_postgres.py")