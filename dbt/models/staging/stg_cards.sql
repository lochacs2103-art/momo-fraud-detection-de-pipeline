{{ config(materialized='view') }}

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
    _ingested_at,
    _batch_id,
    card_brand_part,
    expires_year_part
FROM hive.staging.cards
WHERE card_id IS NOT NULL
