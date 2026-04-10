@"
{{
  config(
    materialized='view',
    tags=['intermediate', 'customer']
  )
}}

WITH customer_orders AS (
    SELECT
        o.customer_id,
        o.order_date,
        o.total_amount,
        o.is_fraud,
        c.customer_segment,
        c.lifetime_value,
        c.total_orders
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
),

customer_metrics AS (
    SELECT
        customer_id,
        customer_segment,
        lifetime_value,
        total_orders,
        COUNT(CASE WHEN is_fraud THEN 1 END) AS fraud_orders,
        ROUND(
            COUNT(CASE WHEN is_fraud THEN 1 END)::NUMERIC / 
            NULLIF(COUNT(*), 0) * 100, 
            2
        ) AS fraud_rate_pct
    FROM customer_orders
    GROUP BY customer_id, customer_segment, lifetime_value, total_orders
)

SELECT
    customer_id,
    customer_segment,
    lifetime_value,
    total_orders,
    fraud_orders,
    fraud_rate_pct,
    CASE
        WHEN fraud_rate_pct > 10 THEN 'high_risk'
        WHEN fraud_rate_pct BETWEEN 5 AND 10 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS risk_category
FROM customer_metrics
"@ | Out-File -FilePath "models/intermediate/int_customer_360.sql" -Encoding utf8