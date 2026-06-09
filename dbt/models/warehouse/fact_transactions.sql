-- fact_transactions.sql
-- Incremental model: merge mỗi ngày, không rebuild toàn bộ
-- unique_key = transaction_id → UPDATE nếu đã có (late arriving fraud label)

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

SELECT
    t.transaction_id,
    t.transaction_date,
    t.user_id,
    t.card_id,
    t.merchant_id,
    t.merchant_city,
    t.merchant_state,
    t.amount,
    t.amount_currency,
    t.mcc,
    t.mcc_description,
    t.card_brand,
    t.card_type,
    t.use_chip,
    t.is_fraud,
    t.is_valid,
    t._batch_id,
    t.year,
    t.month,
    t.day

FROM {{ ref('stg_transactions') }} t

{% if is_incremental() %}
-- Incremental: process hôm nay + 3 ngày trước (để catch late fraud labels)
WHERE t.year  * 10000 + t.month * 100 + t.day >=
      CAST(DATE_FORMAT(DATE_ADD('day', -3, CURRENT_DATE), '%Y%m%d') AS INT)
{% endif %}
