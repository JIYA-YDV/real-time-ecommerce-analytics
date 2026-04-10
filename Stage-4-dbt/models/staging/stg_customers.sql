@"
{{
  config(
    materialized='view',
    tags=['staging', 'customers']
  )
}}

WITH customer_summary AS (
    SELECT
        customer_id,
        MIN(order_timestamp) AS first_order_date,
        MAX(order_timestamp) AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS lifetime_value,
        AVG(total_amount) AS avg_order_value,
        MAX(customer_age_days) AS current_age_days
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

SELECT
    customer_id,
    first_order_date,
    last_order_date,
    total_orders,
    ROUND(lifetime_value, 2) AS lifetime_value,
    ROUND(avg_order_value, 2) AS avg_order_value,
    current_age_days,
    CASE
        WHEN current_age_days < 30 THEN 'new'
        WHEN current_age_days BETWEEN 30 AND 90 THEN 'active'
        WHEN current_age_days > 90 THEN 'loyal'
    END AS customer_segment
FROM customer_summary
"@ | Out-File -FilePath "models/staging/stg_customers.sql" -Encoding utf8