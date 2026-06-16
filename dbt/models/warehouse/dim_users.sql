-- dim_users.sql
-- Đọc từ stg_users (staging view) thay vì hive.warehouse.dim_users
-- Lý do: warehouse.dim_users Parquet có schema conflict với Hive DDL
-- dbt tự tạo table mới với schema đúng từ staging

{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

SELECT
    user_id,
    CAST(current_age AS INTEGER)             AS current_age,
    age_group,
    CAST(retirement_age AS INTEGER)          AS retirement_age,
    gender,
    address,
    CAST(latitude AS DOUBLE)                 AS latitude,
    CAST(longitude AS DOUBLE)                AS longitude,
    CAST(per_capita_income AS DOUBLE)        AS per_capita_income,
    CAST(yearly_income AS DOUBLE)            AS yearly_income,
    CAST(total_debt AS DOUBLE)               AS total_debt,
    CAST(credit_score AS INTEGER)            AS credit_score,
    credit_score_band,
    is_invalid_credit_score,
    CAST(num_credit_cards AS INTEGER)        AS num_credit_cards,
    -- SCD2 columns: dbt incremental = SCD1 behavior
    -- Full SCD2 history kept in hive.warehouse.dim_users (Spark-built)
    CAST(NULL AS timestamp)                  AS valid_from,
    CAST(NULL AS timestamp)                  AS valid_to,
    CAST(true AS boolean)                    AS is_current,
    _batch_id

FROM {{ ref('stg_users') }}

{% if is_incremental() %}
WHERE _batch_id = (SELECT MAX(_batch_id) FROM {{ this }})
{% endif %}
