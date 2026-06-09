-- Test: không có NULL transaction_id trong fact_transactions
-- Nếu query trả về rows → test FAIL

SELECT transaction_id
FROM {{ ref('fact_transactions') }}
WHERE transaction_id IS NULL
