-- dim_cards.sql — SCD Type 1 (overwrite khi có thay đổi)
-- Cards ít thay đổi hơn users, overwrite là đủ

{{ config(materialized='incremental', unique_key='card_id') }}

SELECT
    card_id,
    user_id,
    card_brand,
    card_type,
    card_number_masked,
    expires_month,
    expires_year,
    has_chip,
    num_cards_issued,
    credit_limit,
    acct_open_date,
    year_pin_last_changed,
    card_on_dark_web,
    _batch_id
FROM {{ ref('stg_cards') }}
WHERE card_id IS NOT NULL
