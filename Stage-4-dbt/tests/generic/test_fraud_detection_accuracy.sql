@"
-- Test that fraud detection logic matches expected criteria
-- Orders >$1000 from customers <30 days old should be flagged

SELECT
    order_id,
    total_amount,
    customer_age_days,
    is_fraud
FROM {{ ref('stg_orders') }}
WHERE
    (total_amount > 1000 AND customer_age_days < 30 AND is_fraud = FALSE)
    OR
    (total_amount <= 1000 AND customer_age_days >= 30 AND is_fraud = TRUE)
"@ | Out-File -FilePath "tests/generic/test_fraud_detection_accuracy.sql" -Encoding utf8