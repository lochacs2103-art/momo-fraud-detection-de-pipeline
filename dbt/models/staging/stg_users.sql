{{ config(materialized='view') }}

SELECT
    user_id,
    current_age,                                      -- INT
    age_group,
    retirement_age,                                   -- INT
    CAST(birth_year AS INTEGER)       AS birth_year,  -- STRING in Parquet
    CAST(birth_month AS INTEGER)      AS birth_month, -- STRING in Parquet
    gender,
    address,
    CAST(latitude AS DOUBLE)          AS latitude,    -- STRING in Parquet
    CAST(longitude AS DOUBLE)         AS longitude,   -- STRING in Parquet
    per_capita_income,                                -- DOUBLE
    yearly_income,                                    -- DOUBLE
    total_debt,                                       -- DOUBLE
    credit_score,                                     -- INT
    credit_score_band,
    is_invalid_credit_score,
    CAST(num_credit_cards AS INTEGER) AS num_credit_cards, -- STRING in Parquet
    _ingested_at,
    _batch_id,
    created_year,
    created_month
FROM hive.staging.users
WHERE user_id IS NOT NULL
