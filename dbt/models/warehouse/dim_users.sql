-- dim_users.sql
--
-- ⚠ SCD Type 2 NOTE:
-- dbt incremental với unique_key='user_id' thực hiện MERGE/UPSERT —
-- tức là UPDATE row cũ khi user_id đã tồn tại → đây là SCD Type 1 behavior.
--
-- SCD Type 2 thật sự (expire old rows, insert new rows với valid_from/valid_to)
-- được implement trong: transformation/warehouse/build_dim_users.py (Spark job)
-- Spark job đó chạy TRƯỚC dbt trong Airflow DAG.
--
-- Model này chỉ là VIEW/incremental để Trino + Superset query được dim_users
-- sau khi Spark job đã build đúng SCD2 structure trên HDFS.
-- → Không duplicate logic, không conflict.

{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

-- Đọc từ HDFS path mà build_dim_users.py (Spark SCD2) đã write
-- Chỉ expose current rows để downstream (fact joins, Superset) dùng
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
    valid_from,
    valid_to,
    is_current,
    _batch_id

FROM hive.warehouse.dim_users

{% if is_incremental() %}
-- Chỉ sync rows được Spark job update trong batch gần nhất
WHERE _batch_id = (SELECT MAX(_batch_id) FROM hive.warehouse.dim_users)
{% endif %}
