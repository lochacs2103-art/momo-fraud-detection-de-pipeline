{{ config(materialized='view') }}

SELECT
    user_id,
    CAST(current_age AS INTEGER)      AS current_age,
    age_group,
    CAST(retirement_age AS INTEGER)   AS retirement_age,
    CAST(birth_year AS INTEGER)       AS birth_year,
    CAST(birth_month AS INTEGER)      AS birth_month,
    gender,
    address,
    CAST(latitude AS DOUBLE)          AS latitude,
    CAST(longitude AS DOUBLE)         AS longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    CAST(credit_score AS INTEGER)     AS credit_score,
    credit_score_band,
    is_invalid_credit_score,
    CAST(num_credit_cards AS INTEGER) AS num_credit_cards,
    _ingested_at,
    _batch_id,
    created_year,
    created_month
FROM hive.staging.users
WHERE user_id IS NOT NULL
