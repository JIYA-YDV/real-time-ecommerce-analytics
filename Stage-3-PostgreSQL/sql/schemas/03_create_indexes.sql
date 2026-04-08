-- ============================================
-- INDEXES FOR RAW TABLES
-- ============================================

-- Indexes on raw.orders for faster loading verification
CREATE INDEX IF NOT EXISTS idx_raw_orders_timestamp 
ON raw.orders(order_timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_orders_partition 
ON raw.orders(year, month, day);

CREATE INDEX IF NOT EXISTS idx_raw_orders_customer 
ON raw.orders(customer_id);

-- Partial index for fraud detection
CREATE INDEX IF NOT EXISTS idx_raw_orders_fraud 
ON raw.orders(is_fraud, amount) 
WHERE is_fraud = TRUE;

-- ============================================
-- PARTITIONING (Optional - for large datasets)
-- ============================================

-- If data grows beyond 10M rows, consider partitioning by date
-- Example (commented out for now):

/*
-- Convert raw.orders to partitioned table
CREATE TABLE raw.orders_partitioned (
    LIKE raw.orders INCLUDING ALL
) PARTITION BY RANGE (order_timestamp);

-- Create monthly partitions
CREATE TABLE raw.orders_2024_01 
PARTITION OF raw.orders_partitioned
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE raw.orders_2024_02 
PARTITION OF raw.orders_partitioned
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
*/

-- ============================================
-- MATERIALIZED VIEWS (For faster queries)
-- ============================================

-- Hourly aggregations (refreshed by scheduled job)
DROP MATERIALIZED VIEW IF EXISTS analytics.hourly_metrics CASCADE;

CREATE MATERIALIZED VIEW analytics.hourly_metrics AS
SELECT 
    DATE_TRUNC('hour', order_timestamp) as hour,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value,
    COUNT(*) FILTER (WHERE is_fraud = TRUE) as fraud_count,
    SUM(amount) FILTER (WHERE is_fraud = TRUE) as fraud_amount
FROM raw.orders
GROUP BY DATE_TRUNC('hour', order_timestamp);

-- Create index on materialized view
CREATE UNIQUE INDEX idx_hourly_metrics_hour 
ON analytics.hourly_metrics(hour);

COMMENT ON MATERIALIZED VIEW analytics.hourly_metrics 
IS 'Hourly order metrics (refresh every hour)';

-- ============================================
-- VACUUM AND ANALYZE
-- ============================================

-- Optimize table statistics
VACUUM ANALYZE raw.orders;
VACUUM ANALYZE staging.stg_orders;

-- Show table sizes and index usage
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                   pg_relation_size(schemaname||'.'||tablename)) as indexes_size
FROM pg_tables 
WHERE schemaname IN ('raw', 'staging', 'analytics')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;