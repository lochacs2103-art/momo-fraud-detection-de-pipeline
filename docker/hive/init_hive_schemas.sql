-- init_hive_schemas.sql
-- Tạo Hive databases và external tables
-- Chạy sau khi HDFS directories đã được tạo (make hdfs-init)
-- Usage: beeline -u jdbc:hive2://hive-server:10000 -f init_hive_schemas.sql

-- ============================================================
-- DATABASES
-- ============================================================
CREATE DATABASE IF NOT EXISTS raw
    COMMENT 'Raw layer — exact copy from source DB, append-only'
    LOCATION 'hdfs://namenode:9000/data/lake/raw/';

CREATE DATABASE IF NOT EXISTS staging
    COMMENT 'Staging layer — cleaned, validated, enriched'
    LOCATION 'hdfs://namenode:9000/data/lake/staging/';

CREATE DATABASE IF NOT EXISTS warehouse
    COMMENT 'Warehouse layer — analytics-ready facts, dims, features'
    LOCATION 'hdfs://namenode:9000/data/lake/warehouse/';

-- ============================================================
-- STAGING TABLES
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS staging.transactions (
    transaction_id          STRING,
    transaction_date        TIMESTAMP,
    user_id                 STRING,
    card_id                 STRING,

    -- Amount: 5 cột từ AmountParser
    amount_raw              STRING,
    amount                  DOUBLE,
    amount_currency         STRING,
    amount_format           STRING,    -- US|EU|CLEAN|VN|AMBIGUOUS|INVALID
    amount_parse_note       STRING,

    -- Amount flag
    is_refund               BOOLEAN,   -- TRUE khi amount < 0

    -- use_chip: encoded INT (0=SWIPE, 1=CHIP, 2=ONLINE) + raw string để audit
    use_chip                INT,
    use_chip_raw            STRING,

    -- Online transaction flag
    is_online_transaction   BOOLEAN,   -- TRUE khi merchant_city=ONLINE

    -- Merchant
    merchant_id             STRING,
    merchant_city           STRING,
    merchant_state          STRING,    -- 'ONLINE' nếu online transaction
    zip                     STRING,    -- 5-digit string hoặc 'ONLINE'
    mcc                     INT,
    mcc_description         STRING,    -- từ mcc_codes lookup, 'UNKNOWN' nếu không khớp

    -- Card info (enriched từ dim_cards)
    card_brand              STRING,
    card_type               STRING,
    card_number_masked      STRING,

    -- Errors: exploded thành boolean columns
    error_bad_pin               BOOLEAN,
    error_bad_cvv               BOOLEAN,
    error_bad_card_number       BOOLEAN,
    error_bad_expiration        BOOLEAN,
    error_bad_zipcode           BOOLEAN,
    error_insufficient_balance  BOOLEAN,
    error_technical_glitch      BOOLEAN,
    has_error                   BOOLEAN,   -- TRUE nếu bất kỳ error nào = TRUE

    -- Fraud label
    is_fraud                BOOLEAN,

    -- Quality flag
    is_valid                BOOLEAN,

    -- Metadata
    _ingested_at            TIMESTAMP,
    _source_file            STRING,
    _batch_id               STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.users (
    user_id             STRING,
    current_age         INT,
    age_group           STRING,    -- TEEN|YOUNG_ADULT|ADULT|MIDDLE_AGED|SENIOR
    retirement_age      INT,
    birth_year          INT,
    birth_month         INT,
    gender              STRING,
    address             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    per_capita_income   DOUBLE,
    yearly_income       DOUBLE,
    total_debt          DOUBLE,
    credit_score        INT,
    credit_score_band   STRING,    -- POOR|FAIR|GOOD|VERY_GOOD|EXCEPTIONAL|INVALID
    is_invalid_credit_score BOOLEAN,
    num_credit_cards    INT,
    is_valid            BOOLEAN,
    _ingested_at        TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
PARTITIONED BY (created_year INT, created_month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/users/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.cards (
    card_id                 STRING,
    user_id                 STRING,
    card_brand              STRING,
    card_type               STRING,
    card_number_masked      STRING,
    expires_month           INT,
    expires_year            INT,
    has_chip                BOOLEAN,
    num_cards_issued        INT,
    credit_limit            DOUBLE,
    acct_open_date          STRING,
    account_age_months      INT,       -- số tháng từ acct_open_date đến ngày ingest
    year_pin_last_changed   INT,
    card_on_dark_web        BOOLEAN,
    is_valid                BOOLEAN,
    _ingested_at            TIMESTAMP,
    _source_file            STRING,
    _batch_id               STRING
)
PARTITIONED BY (card_brand_part STRING, expires_year_part INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/cards/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.mcc_codes (
    mcc             INT,
    mcc_code        STRING,
    mcc_description STRING,
    _ingested_at    TIMESTAMP,
    _source_file    STRING,
    _batch_id       STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/mcc_codes/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.transactions_quarantine (
    transaction_id      STRING,
    amount_raw          STRING,
    amount_format       STRING,
    amount_parse_note   STRING,
    quarantine_reason   STRING,
    quarantine_ts       TIMESTAMP,
    resolved_by         STRING,
    resolved_at         TIMESTAMP,
    is_resolved         BOOLEAN,
    _batch_id           STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/quarantine/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ============================================================
-- WAREHOUSE TABLES
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.fact_transactions (
    transaction_id          STRING,
    transaction_date        TIMESTAMP,
    user_id                 STRING,
    card_id                 STRING,
    merchant_id             STRING,
    merchant_city           STRING,
    merchant_state          STRING,
    amount                  DOUBLE,
    amount_currency         STRING,
    is_refund               BOOLEAN,
    is_online_transaction   BOOLEAN,
    mcc                     INT,
    mcc_description         STRING,
    card_brand              STRING,
    card_type               STRING,
    use_chip                INT,       -- 0=SWIPE, 1=CHIP, 2=ONLINE
    -- Error flags
    error_bad_pin               BOOLEAN,
    error_bad_cvv               BOOLEAN,
    error_bad_card_number       BOOLEAN,
    error_bad_expiration        BOOLEAN,
    error_bad_zipcode           BOOLEAN,
    error_insufficient_balance  BOOLEAN,
    error_technical_glitch      BOOLEAN,
    has_error                   BOOLEAN,
    is_fraud                BOOLEAN,
    is_valid                BOOLEAN,
    _batch_id               STRING
)
PARTITIONED BY (year INT, month INT)
CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/fact_transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.dim_users (
    user_id                 STRING,
    current_age             INT,
    age_group               STRING,    -- TEEN|YOUNG_ADULT|ADULT|MIDDLE_AGED|SENIOR
    retirement_age          INT,
    gender                  STRING,
    address                 STRING,
    latitude                DOUBLE,
    longitude               DOUBLE,
    per_capita_income       DOUBLE,
    yearly_income           DOUBLE,
    total_debt              DOUBLE,
    credit_score            INT,
    credit_score_band       STRING,    -- POOR|FAIR|GOOD|VERY_GOOD|EXCEPTIONAL|INVALID
    is_invalid_credit_score BOOLEAN,
    num_credit_cards        INT,
    valid_from              TIMESTAMP,
    valid_to                TIMESTAMP,
    is_current              BOOLEAN,
    _batch_id               STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/dim_users/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.dim_cards (
    card_id                 STRING,
    user_id                 STRING,
    card_brand              STRING,
    card_type               STRING,
    card_number_masked      STRING,
    expires_month           INT,
    expires_year            INT,
    has_chip                BOOLEAN,
    credit_limit            DOUBLE,
    account_age_months      INT,
    card_on_dark_web        BOOLEAN,
    _batch_id               STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/dim_cards/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.feat_fraud_features (
    transaction_id              STRING,
    user_id                     STRING,
    txn_count_last_1h           INT,
    txn_count_last_24h          INT,
    txn_count_last_7d           INT,
    amount_sum_last_1h          DOUBLE,
    amount_sum_last_24h         DOUBLE,
    amount_vs_user_avg_ratio    DOUBLE,
    is_night_txn                BOOLEAN,
    is_weekend                  BOOLEAN,
    is_foreign_merchant         BOOLEAN,
    card_on_dark_web            BOOLEAN,
    is_fraud                    BOOLEAN,
    risk_score                  DOUBLE,
    _batch_id                   STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/feat_fraud_features/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.transaction_event_log (
    transaction_id  STRING,
    event_type      STRING,
    event_time      TIMESTAMP,
    event_payload   STRING,
    _ingested_at    TIMESTAMP,
    _batch_id       STRING
)
PARTITIONED BY (event_year INT, event_month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/transaction_event_log/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
