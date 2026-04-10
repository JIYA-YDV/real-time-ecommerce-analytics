@"
{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        price,
        total_amount,
        order_timestamp,
        customer_age_days,
        is_fraud,
        processed_time
    FROM {{ source('raw', 'orders') }}
    WHERE order_timestamp IS NOT NULL
),

cleaned AS (
    SELECT
        order_id::VARCHAR(50) AS order_id,
        customer_id::VARCHAR(50) AS customer_id,
        product_id::VARCHAR(50) AS product_id,
        quantity::INTEGER AS quantity,
        price::DECIMAL(10,2) AS price,
        total_amount::DECIMAL(10,2) AS total_amount,
        order_timestamp::TIMESTAMP AS order_timestamp,
        customer_age_days::INTEGER AS customer_age_days,
        is_fraud::BOOLEAN AS is_fraud,
        processed_time::TIMESTAMP AS processed_time,
        -- Derived fields
        DATE(order_timestamp) AS order_date,
        EXTRACT(HOUR FROM order_timestamp) AS order_hour,
        EXTRACT(DOW FROM order_timestamp) AS order_day_of_week
    FROM source_data
)

SELECT * FROM cleaned

-- Data quality checks
{{ dbt_utils.group_by(n=11) }}
"@ | Out-File -FilePath "models/staging/stg_orders.sql" -Encoding utf8