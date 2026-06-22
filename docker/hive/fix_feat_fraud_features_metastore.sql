-- Sửa metastore cho warehouse.feat_fraud_features (CD_ID có thể khác — kiểm tra trước khi chạy)
--
-- Nguyên nhân lỗi dbt fraud_features:
--   Spark ghi Parquet: amount_sum_* = DECIMAL(28,2), risk_score = DOUBLE
--   Hive metastore bị sửa sai (tất cả thành decimal) → Trino scan Parquet fail
--
-- Chỉ sửa metadata, KHÔNG đụng data HDFS, KHÔNG cần chạy lại Spark/Airflow tasks cũ.
--
-- Bước 1: Tìm CD_ID của bảng feat_fraud_features
--   docker exec -it hive-metastore-db psql -U hive -d metastore -c "
--     SELECT t.\"TBL_NAME\", c.\"CD_ID\"
--     FROM \"TBLS\" t
--     JOIN \"DBS\" d ON t.\"DB_ID\" = d.\"DB_ID\"
--     JOIN \"SDS\" s ON t.\"SD_ID\" = s.\"SD_ID\"
--     JOIN \"COLUMNS_V2\" c ON s.\"CD_ID\" = c.\"CD_ID\"
--     WHERE d.\"NAME\" = 'warehouse' AND t.\"TBL_NAME\" = 'feat_fraud_features'
--     LIMIT 1;
--   "
--
-- Bước 2: Thay <CD_ID> bên dưới rồi chạy UPDATE

-- amount_sum từ Spark SUM(decimal) → Parquet DECIMAL(28,2)
UPDATE "COLUMNS_V2" SET "TYPE_NAME" = 'decimal(28,2)'
WHERE "CD_ID" = <CD_ID>
  AND "COLUMN_NAME" IN ('amount_sum_last_1h', 'amount_sum_last_24h', 'amount_vs_user_avg_ratio');

-- risk_score Spark cast double → Parquet DOUBLE (KHÔNG được để decimal)
UPDATE "COLUMNS_V2" SET "TYPE_NAME" = 'double'
WHERE "CD_ID" = <CD_ID>
  AND "COLUMN_NAME" = 'risk_score';
