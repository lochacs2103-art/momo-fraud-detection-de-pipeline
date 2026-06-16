-- stg_transactions.sql
-- Staging view — select từ Hive staging.transactions
-- Materialized as VIEW → không lưu data, query thẳng từ Parquet

{{ config(materialized='view') }}

SELECT
    transaction_id,
    transaction_date,
    user_id,
    card_id,

    -- Amount (5 cột từ AmountParser)
    amount_raw,
    CAST(amount AS DOUBLE)  AS amount,  -- DECIMAL(18,2) trong Parquet → cast DOUBLE cho dbt/Trino compat
    amount_currency,
    amount_format,
    amount_parse_note,

    -- Amount flags
    is_refund,

    -- use_chip encoded
    use_chip,        -- INT: 0=SWIPE, 1=CHIP, 2=ONLINE
    use_chip_raw,    -- original string để audit

    -- Online transaction
    is_online_transaction,

    -- Merchant
    merchant_id,
    merchant_city,
    merchant_state,
    zip,
    mcc,
    mcc_description,

    -- Card info
    card_brand,
    card_type,
    card_number_masked,

    -- Error flags (exploded từ errors string)
    error_bad_pin,
    error_bad_cvv,
    error_bad_card_number,
    error_bad_expiration,
    error_bad_zipcode,
    error_insufficient_balance,
    error_technical_glitch,
    has_error,

    -- Fraud
    is_fraud,
    is_valid,

    -- Metadata
    _ingested_at,
    _batch_id,
    year,
    month,
    day

FROM hive.staging.transactions
WHERE is_valid = true
