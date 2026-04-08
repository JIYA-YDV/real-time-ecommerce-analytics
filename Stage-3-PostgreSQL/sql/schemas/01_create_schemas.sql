-- Create schemas for data organization
-- Raw schema: Direct copy from Parquet files
-- Staging schema: Cleaned data for dbt transformations (Stage 4)
-- Analytics schema: Business-ready tables (will be created by dbt in Stage 4)

-- Drop existing schemas (for development only)
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS analytics CASCADE;

-- Create schemas
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA analytics;

-- Grant permissions
GRANT ALL ON SCHEMA raw TO ecommerce_user;
GRANT ALL ON SCHEMA staging TO ecommerce_user;
GRANT ALL ON SCHEMA analytics TO ecommerce_user;

-- Add comments
COMMENT ON SCHEMA raw IS 'Raw data loaded from MinIO Parquet files';
COMMENT ON SCHEMA staging IS 'Cleaned and deduplicated data for dbt transformations';
COMMENT ON SCHEMA analytics IS 'Business-ready analytics tables (created by dbt)';

-- Verify schemas created
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'staging', 'analytics')
ORDER BY schema_name;