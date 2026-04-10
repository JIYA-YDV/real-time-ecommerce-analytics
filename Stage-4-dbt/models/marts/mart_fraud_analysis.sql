@"
{{
  config(
    materialized='table',
    tags=['marts', 'fraud']
  )
}}

WITH fraud_patterns AS (
    SELECT
        order_date,
        order_hour,
        customer_segment,
        COUNT(*) AS fraud_count,
        AVG(total_amount) AS avg_fraud_amount,
        MIN(customer_age_days) AS min_customer_age,
        MAX(customer_age_days) AS max_customer_age
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE is_fraud = TRUE
    GROUP BY order_date, order_hour, customer_segment
)

SELECT
    order_date,
    order_hour,
    customer_segment,
    fraud_count,
    ROUND(avg_fraud_amount, 2) AS avg_fraud_amount,
    min_customer_age,
    max_customer_age,
    CASE
        WHEN order_hour BETWEEN 0 AND 6 THEN 'night'
        WHEN order_hour BETWEEN 7 AND 12 THEN 'morning'
        WHEN order_hour BETWEEN 13 AND 18 THEN 'afternoon'
        ELSE 'evening'
    END AS time_of_day
FROM fraud_patterns
ORDER BY fraud_count DESC
"@ | Out-File -FilePath "models/marts/mart_fraud_analysis.sql" -Encoding utf8