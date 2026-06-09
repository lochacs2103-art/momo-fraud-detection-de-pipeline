-- dim_merchants.sql — derived từ transactions + mcc_codes
-- Rebuild mỗi ngày (table materialization)

{{ config(materialized='table') }}

SELECT
    merchant_id,
    MAX(merchant_city)    AS merchant_city,
    MAX(merchant_state)   AS merchant_state,
    MAX(mcc)              AS mcc,
    MAX(mcc_description)  AS mcc_description,

    COUNT(*)                                                       AS total_txn_count,
    SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)              AS fraud_txn_count,
    ROUND(
        SUM(CASE WHEN is_fraud = true THEN 1.0 ELSE 0 END) / COUNT(*),
        4
    )                                                              AS fraud_rate,

    CASE
        WHEN fraud_rate >= 0.10 THEN 'CRITICAL'
        WHEN fraud_rate >= 0.05 THEN 'HIGH'
        WHEN fraud_rate >= 0.02 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                            AS risk_tier,

    AVG(amount)                                                    AS avg_amount,
    CURRENT_TIMESTAMP                                              AS _updated_at

FROM {{ ref('stg_transactions') }}
WHERE merchant_id IS NOT NULL
GROUP BY merchant_id
