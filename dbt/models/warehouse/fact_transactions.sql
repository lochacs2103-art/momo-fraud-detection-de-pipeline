-- fact_transactions.sql
-- Materialized as VIEW — data đã có sẵn trong staging.transactions (Spark-built)
-- Không cần copy 13.3M rows qua Trino → để Spark làm việc đó
-- dbt chỉ expose view với đúng columns cho downstream

{{ config(materialized='view') }}

SELECT
    t.transaction_id,
    t.transaction_date,
    t.user_id,
    t.card_id,
    t.merchant_id,
    t.merchant_city,
    t.merchant_state,
    CAST(t.amount AS DOUBLE)        AS amount,
    t.amount_currency,
    t.is_refund,
    t.is_online_transaction,
    t.mcc,
    t.mcc_description,
    t.card_brand,
    t.card_type,
    t.use_chip,
    t.error_bad_pin,
    t.error_bad_cvv,
    t.error_bad_card_number,
    t.error_bad_expiration,
    t.error_bad_zipcode,
    t.error_insufficient_balance,
    t.error_technical_glitch,
    t.has_error,
    t.is_fraud,
    t.is_valid,
    t._batch_id,
    t.year,
    t.month,
    t.day

FROM {{ ref('stg_transactions') }} t
