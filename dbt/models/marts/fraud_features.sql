-- fraud_features.sql — mart table kết hợp fact + features + dims
-- Dùng cho Superset dashboards và ML team

{{ config(materialized='table') }}

SELECT
    t.transaction_id,
    t.transaction_date,
    t.user_id,
    t.amount,
    t.amount_currency,
    t.mcc_description,
    t.card_brand,
    t.card_type,
    t.merchant_city,
    t.merchant_state,
    t.is_fraud,

    -- User context
    u.current_age,
    u.gender,
    u.credit_score,
    u.yearly_income,

    -- Fraud features
    CAST(f.txn_count_last_1h AS INTEGER)         AS txn_count_last_1h,
    CAST(f.txn_count_last_24h AS INTEGER)        AS txn_count_last_24h,
    CAST(f.txn_count_last_7d AS INTEGER)         AS txn_count_last_7d,
    CAST(f.amount_sum_last_24h AS DOUBLE)        AS amount_sum_last_24h,
    CAST(f.amount_vs_user_avg_ratio AS DOUBLE)   AS amount_vs_user_avg_ratio,
    f.is_night_txn,
    f.is_weekend,
    f.is_foreign_merchant,
    f.card_on_dark_web,
    f.risk_score,

    -- Merchant risk
    m.fraud_rate          AS merchant_fraud_rate,
    m.risk_tier           AS merchant_risk_tier,

    t.year,
    t.month,
    t.day

FROM {{ ref('fact_transactions') }} t
LEFT JOIN {{ ref('dim_users') }} u
    ON t.user_id = u.user_id AND u.is_current = true
LEFT JOIN hive.warehouse.feat_fraud_features f
    ON t.transaction_id = f.transaction_id
LEFT JOIN {{ ref('dim_merchants') }} m
    ON t.merchant_id = m.merchant_id
