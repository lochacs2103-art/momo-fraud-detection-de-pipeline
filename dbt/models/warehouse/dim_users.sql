-- dim_users.sql — đọc từ stg_users với đúng types

{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='append',
    on_schema_change='sync_all_columns'
) }}

SELECT
    user_id,
    current_age,
    age_group,
    retirement_age,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    credit_score_band,
    is_invalid_credit_score,
    num_credit_cards,
    CAST(NULL AS timestamp) AS valid_from,
    CAST(NULL AS timestamp) AS valid_to,
    CAST(true AS boolean)   AS is_current,
    _batch_id

FROM {{ ref('stg_users') }}

{% if is_incremental() %}
WHERE _batch_id = (SELECT MAX(_batch_id) FROM {{ this }})
{% endif %}
