-- ============================================
-- RAW TABLES (Direct copy from Parquet)
-- ============================================

-- Raw orders table (matches Spark output schema)
DROP TABLE IF EXISTS raw.orders CASCADE;

CREATE TABLE raw.orders (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    quantity INTEGER,
    amount DECIMAL(12, 2),
    order_timestamp TIMESTAMP NOT NULL,
    customer_age_days INTEGER,
    is_fraud BOOLEAN,
    fraud_reason VARCHAR(255),
    
    -- Partition columns from Spark
    year INTEGER,
    month INTEGER,
    day INTEGER,
    
    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500)
);

-- Add comments
COMMENT ON TABLE raw.orders IS 'Raw orders loaded from MinIO Parquet files';
COMMENT ON COLUMN raw.orders.customer_age_days IS 'Days since customer registration';
COMMENT ON COLUMN raw.orders.is_fraud IS 'Fraud flag from Spark streaming job';

-- ============================================
-- STAGING TABLES (Cleaned for dbt)
-- ============================================

-- Staging orders (deduplicated and cleaned)
DROP TABLE IF EXISTS staging.stg_orders CASCADE;

CREATE TABLE staging.stg_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2) CHECK (price >= 0),
    quantity INTEGER CHECK (quantity > 0),
    amount DECIMAL(12, 2) CHECK (amount >= 0),
    order_timestamp TIMESTAMP NOT NULL,
    customer_age_days INTEGER CHECK (customer_age_days >= 0),
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_reason VARCHAR(255),
    
    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments
COMMENT ON TABLE staging.stg_orders IS 'Cleaned and deduplicated orders for dbt transformations';

-- Create indexes on staging table
CREATE INDEX idx_stg_orders_customer ON staging.stg_orders(customer_id);
CREATE INDEX idx_stg_orders_timestamp ON staging.stg_orders(order_timestamp);
CREATE INDEX idx_stg_orders_category ON staging.stg_orders(category);
CREATE INDEX idx_stg_orders_fraud ON staging.stg_orders(is_fraud) WHERE is_fraud = TRUE;

-- ============================================
-- ANALYTICS TABLES (Placeholder - dbt will create)
-- ============================================

-- These will be created by dbt in Stage 4
-- Shown here for reference

-- Daily revenue aggregation
DROP TABLE IF EXISTS analytics.daily_revenue CASCADE;

CREATE TABLE analytics.daily_revenue (
    date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(15, 2),
    avg_order_value DECIMAL(10, 2),
    fraud_orders INTEGER,
    fraud_amount DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE analytics.daily_revenue IS 'Daily revenue metrics (will be managed by dbt)';

-- Customer analytics
DROP TABLE IF EXISTS analytics.customer_metrics CASCADE;

CREATE TABLE analytics.customer_metrics (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_order_date DATE,
    last_order_date DATE,
    total_orders INTEGER,
    total_spent DECIMAL(15, 2),
    avg_order_value DECIMAL(10, 2),
    is_high_value BOOLEAN,
    fraud_orders INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE analytics.customer_metrics IS 'Customer lifetime value metrics (will be managed by dbt)';

-- Verify tables created
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname IN ('raw', 'staging', 'analytics')
ORDER BY schemaname, tablename;
