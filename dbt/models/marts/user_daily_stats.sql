-- user_daily_stats.sql — aggregate per user per day

{{ config(materialized='table') }}

SELECT
    user_id,
    year,
    month,
    day,
    DATE(transaction_date)                                    AS txn_date,
    COUNT(*)                                                  AS total_txn_count,
    SUM(amount)                                               AS total_amount,
    AVG(amount)                                               AS avg_amount,
    MAX(amount)                                               AS max_amount,
    SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)          AS fraud_txn_count,
    COUNT(DISTINCT merchant_id)                               AS distinct_merchants

FROM {{ ref('fact_transactions') }}
WHERE is_valid = true
GROUP BY user_id, year, month, day, DATE(transaction_date)
