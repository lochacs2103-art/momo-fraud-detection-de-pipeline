-- fact_transactions.sql
-- Incremental model: merge mỗi ngày, không rebuild toàn bộ
-- unique_key = transaction_id → UPDATE nếu đã có (late arriving fraud label)
--
-- FIX: dùng var('execution_date') từ Airflow thay vì CURRENT_DATE
-- Lý do: CURRENT_DATE sẽ fail backfill — nếu backfill tháng cũ,
-- điều kiện CURRENT_DATE - 3 days sẽ bỏ qua toàn bộ data cũ.
-- execution_date được Airflow truyền vào đúng ngày đang process.
--
-- Airflow gọi dbt với:
--   dbt run --vars '{"execution_date": "2023-06-15"}'
-- Backfill sẽ tự động truyền đúng execution_date cho từng DAG run.

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='append',
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
    CAST(t.amount AS DOUBLE)        AS amount,
    t.amount_currency,
    t.is_refund,
    t.is_online_transaction,
    t.mcc,
    t.mcc_description,
    t.card_brand,
    t.card_type,
    t.use_chip,
    t.error_bad_pin,
    t.error_bad_cvv,
    t.error_bad_card_number,
    t.error_bad_expiration,
    t.error_bad_zipcode,
    t.error_insufficient_balance,
    t.error_technical_glitch,
    t.has_error,
    t.is_fraud,
    t.is_valid,
    t._batch_id,
    t.year,
    t.month,
    t.day

FROM {{ ref('stg_transactions') }} t

{% if is_incremental() %}
-- Dùng execution_date từ Airflow var, không dùng CURRENT_DATE.
-- Lookback 3 ngày để catch late arriving fraud labels.
-- Khi backfill: Airflow truyền đúng execution_date của từng ngày → không bỏ sót.
WHERE CAST(
    DATE_FORMAT(
        DATE_ADD('day', -3,
            DATE_PARSE('{{ var("execution_date", "") }}', '%Y-%m-%d')
        ),
        '%Y%m%d'
    ) AS INT
) <= t.year * 10000 + t.month * 100 + t.day
AND t.year * 10000 + t.month * 100 + t.day <= CAST(
    DATE_FORMAT(
        DATE_PARSE('{{ var("execution_date", "") }}', '%Y-%m-%d'),
        '%Y%m%d'
    ) AS INT
)
{% endif %}
