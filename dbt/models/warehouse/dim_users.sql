-- dim_users.sql — SCD Type 2
-- Mỗi khi user thay đổi info → row mới với valid_from mới
-- Row cũ: valid_to = now(), is_current = false
-- Row mới: valid_to = '9999-12-31', is_current = true

{{ config(materialized='incremental', unique_key='user_id') }}

WITH current_snapshot AS (
    SELECT
        user_id,
        current_age,
        retirement_age,
        gender,
        address,
        latitude,
        longitude,
        per_capita_income,
        yearly_income,
        total_debt,
        credit_score,
        num_credit_cards,
        _batch_id
    FROM {{ ref('stg_users') }}
),

{% if is_incremental() %}
-- So sánh với existing dim để detect changes
existing AS (
    SELECT * FROM {{ this }}
    WHERE is_current = true
),

changed AS (
    SELECT
        n.user_id,
        n.current_age,
        n.retirement_age,
        n.gender,
        n.address,
        n.latitude,
        n.longitude,
        n.per_capita_income,
        n.yearly_income,
        n.total_debt,
        n.credit_score,
        n.num_credit_cards,
        n._batch_id,
        -- Detect nếu có gì thay đổi
        (
            COALESCE(e.address, '')          != COALESCE(n.address, '')          OR
            COALESCE(e.yearly_income, 0)     != COALESCE(n.yearly_income, 0)     OR
            COALESCE(e.credit_score, 0)      != COALESCE(n.credit_score, 0)      OR
            COALESCE(e.total_debt, 0)        != COALESCE(n.total_debt, 0)
        ) AS is_changed
    FROM current_snapshot n
    LEFT JOIN existing e USING (user_id)
)
{% else %}
changed AS (
    SELECT *, true AS is_changed FROM current_snapshot
)
{% endif %}

SELECT
    user_id,
    current_age,
    retirement_age,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards,
    CURRENT_TIMESTAMP          AS valid_from,
    TIMESTAMP '9999-12-31 00:00:00' AS valid_to,
    true                       AS is_current,
    _batch_id
FROM changed
WHERE is_changed = true
