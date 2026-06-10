-- ============================================================
-- Load data từ CSV/JSON vào source database
-- 
-- CSV files phải được copy vào docker/source-db/init/data/ trước khi
-- chạy docker compose up.
--
-- Chạy lệnh này trước: make copy-data
-- Hoặc thủ công:
--   mkdir -p docker/source-db/init/data
--   cp data/raw/* docker/source-db/init/data/
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
    NULL ''
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
    NULL ''
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
    NULL ''
);
\echo 'Cards loaded.'

-- ---- Load MCC codes từ JSON ----
\echo 'Loading mcc_codes.json...'
INSERT INTO raw_mcc_codes (mcc_code, description)
SELECT
    key   AS mcc_code,
    TRIM(BOTH '"' FROM value::TEXT) AS description
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
-- NOTE: fraud_labels.json quá lớn cho pg_read_file()
-- Dùng script Python thay thế: python docker/source-db/load_fraud_labels.py
\echo 'Skipping fraud labels (use load_fraud_labels.py instead)'

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
