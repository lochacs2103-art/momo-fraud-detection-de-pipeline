{{ config(materialized='view') }}

SELECT
    user_id,
    current_age,
    age_group,               -- TEEN|YOUNG_ADULT|ADULT|MIDDLE_AGED|SENIOR
    retirement_age,
    birth_year,
    birth_month,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    credit_score_band,       -- POOR|FAIR|GOOD|VERY_GOOD|EXCEPTIONAL|INVALID
    is_invalid_credit_score,
    num_credit_cards,
    _ingested_at,
    _batch_id,
    created_year,
    created_month
FROM hive.staging.users
WHERE user_id IS NOT NULL
