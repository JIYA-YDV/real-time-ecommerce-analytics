@"
{{
  config(
    materialized='table',
    tags=['marts', 'revenue']
  )
}}

SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    SUM(CASE WHEN is_fraud THEN total_amount ELSE 0 END) AS fraud_revenue,
    COUNT(CASE WHEN is_fraud THEN 1 END) AS fraud_orders,
    ROUND(
        SUM(CASE WHEN is_fraud THEN total_amount ELSE 0 END) / 
        NULLIF(SUM(total_amount), 0) * 100,
        2
    ) AS fraud_revenue_pct
FROM {{ ref('stg_orders') }}
GROUP BY order_date
ORDER BY order_date DESC
"@ | Out-File -FilePath "models/marts/mart_daily_revenue.sql" -Encoding utf8