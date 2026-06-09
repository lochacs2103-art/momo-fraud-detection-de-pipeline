-- merchant_risk_score.sql — monthly merchant risk aggregation

{{ config(materialized='table') }}

SELECT
    merchant_id,
    year,
    month,
    mcc_description,
    COUNT(*)                                                   AS total_txn_count,
    SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)           AS fraud_txn_count,
    ROUND(
        SUM(CASE WHEN is_fraud = true THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0),
        4
    )                                                          AS fraud_rate,
    AVG(amount)                                                AS avg_amount,
    CASE
        WHEN ROUND(SUM(CASE WHEN is_fraud = true THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*),0), 4) >= 0.10 THEN 'CRITICAL'
        WHEN ROUND(SUM(CASE WHEN is_fraud = true THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*),0), 4) >= 0.05 THEN 'HIGH'
        WHEN ROUND(SUM(CASE WHEN is_fraud = true THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*),0), 4) >= 0.02 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                        AS risk_tier

FROM {{ ref('fact_transactions') }}
WHERE is_valid = true
GROUP BY merchant_id, year, month, mcc_description
