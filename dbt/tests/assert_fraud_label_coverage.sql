-- Test: ít nhất 80% transactions phải có fraud label
-- Nếu coverage < 80% → có vấn đề với fraud label ingestion

WITH stats AS (
    SELECT
        COUNT(*)                                          AS total,
        SUM(CASE WHEN is_fraud IS NOT NULL THEN 1 END)   AS labeled
    FROM {{ ref('fact_transactions') }}
    WHERE year = YEAR(CURRENT_DATE) - 1   -- check năm trước (đã có đủ labels)
)
SELECT *
FROM stats
WHERE labeled * 1.0 / NULLIF(total, 0) < 0.80
