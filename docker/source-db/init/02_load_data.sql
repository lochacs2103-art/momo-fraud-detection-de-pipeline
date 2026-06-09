-- ============================================================
-- Load data từ CSV/JSON vào source database
-- Script này chạy tự động khi PostgreSQL container khởi động lần đầu
-- Files CSV được mount vào /docker-entrypoint-initdb.d/data/
--
-- COPY là lệnh PostgreSQL để bulk load từ file — nhanh hơn INSERT nhiều lần
-- ============================================================

-- ---- Load transactions ----
\echo 'Loading transactions_data.csv...'
COPY raw_transactions (
    id, date, client_id, card_id, amount, use_chip,
    merchant_id, merchant_city, merchant_state, zip, mcc, errors
)
FROM '/docker-entrypoint-initdb.d/data/transactions_data.csv'
WITH (
    FORMAT csv,
    HEADER true,
    NULL '',
    QUOTE '"',
    ESCAPE '\'
);
\echo 'Transactions loaded.'

-- ---- Load users ----
\echo 'Loading users_data.csv...'
COPY raw_users (
    id, current_age, retirement_age, birth_year, birth_month,
    gender, address, latitude, longitude,
    per_capita_income, yearly_income, total_debt,
    credit_score, num_credit_cards
)
FROM '/docker-entrypoint-initdb.d/data/users_data.csv'
WITH (
    FORMAT csv,
    HEADER true,
    NULL '',
    QUOTE '"',
    ESCAPE '\'
);
\echo 'Users loaded.'

-- ---- Load cards ----
\echo 'Loading cards_data.csv...'
COPY raw_cards (
    id, client_id, card_brand, card_type, card_number,
    expires, cvv, has_chip, num_cards_issued, credit_limit,
    acct_open_date, year_pin_last_changed, card_on_dark_web
)
FROM '/docker-entrypoint-initdb.d/data/cards_data.csv'
WITH (
    FORMAT csv,
    HEADER true,
    NULL '',
    QUOTE '"',
    ESCAPE '\'
);
\echo 'Cards loaded.'

-- ---- Load MCC codes từ JSON → cần dùng psql function ----
-- mcc_codes.json có dạng: {"0742": "Veterinary Services", ...}
-- PostgreSQL có thể đọc JSON và flatten thành rows
\echo 'Loading mcc_codes.json...'
INSERT INTO raw_mcc_codes (mcc_code, description)
SELECT
    key   AS mcc_code,
    value AS description
FROM (
    SELECT
        json_object_keys(content::json) AS key,
        content::json -> json_object_keys(content::json) AS value
    FROM (
        SELECT pg_read_file('/docker-entrypoint-initdb.d/data/mcc_codes.json') AS content
    ) t
) kv
ON CONFLICT (mcc_code) DO NOTHING;
\echo 'MCC codes loaded.'

-- ---- Load fraud labels từ JSON ----
-- train_fraud_labels.json có dạng: {"transaction_id": "Yes", "txn_id2": "No", ...}
-- Value là "Yes"/"No" string, KHÔNG phải 0/1
\echo 'Loading train_fraud_labels.json...'
INSERT INTO raw_fraud_labels (transaction_id, is_fraud)
SELECT
    key            AS transaction_id,
    TRIM(BOTH '"' FROM value::TEXT) AS is_fraud   -- strip quotes: '"Yes"' → 'Yes'
FROM (
    SELECT
        json_object_keys(content::json) AS key,
        content::json -> json_object_keys(content::json) AS value
    FROM (
        SELECT pg_read_file('/docker-entrypoint-initdb.d/data/train_fraud_labels.json') AS content
    ) t
) kv
ON CONFLICT (transaction_id) DO NOTHING;
\echo 'Fraud labels loaded.'

-- ---- Summary ----
\echo ''
\echo '=== Load Summary ==='
SELECT 'raw_transactions' AS table_name, COUNT(*) AS row_count FROM raw_transactions
UNION ALL
SELECT 'raw_users',        COUNT(*) FROM raw_users
UNION ALL
SELECT 'raw_cards',        COUNT(*) FROM raw_cards
UNION ALL
SELECT 'raw_mcc_codes',    COUNT(*) FROM raw_mcc_codes
UNION ALL
SELECT 'raw_fraud_labels', COUNT(*) FROM raw_fraud_labels;
