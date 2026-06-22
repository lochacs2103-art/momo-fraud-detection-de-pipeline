CREATE DATABASE IF NOT EXISTS raw
    LOCATION 'hdfs://namenode:9000/data/lake/raw/';

CREATE DATABASE IF NOT EXISTS staging
    LOCATION 'hdfs://namenode:9000/data/lake/staging/';

CREATE DATABASE IF NOT EXISTS warehouse
    LOCATION 'hdfs://namenode:9000/data/lake/warehouse/';

CREATE EXTERNAL TABLE IF NOT EXISTS staging.transactions (
    transaction_id          STRING,
    transaction_date        TIMESTAMP,
    user_id                 STRING,
    card_id                 STRING,
    amount_raw              STRING,
    amount                  DOUBLE,
    amount_currency         STRING,
    amount_format           STRING,
    amount_parse_note       STRING,
    is_refund               BOOLEAN,
    use_chip                INT,
    use_chip_raw            STRING,
    is_online_transaction   BOOLEAN,
    merchant_id             STRING,
    merchant_city           STRING,
    merchant_state          STRING,
    zip                     STRING,
    mcc                     INT,
    mcc_description         STRING,
    card_brand              STRING,
    card_type               STRING,
    card_number_masked      STRING,
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
    _ingested_at            TIMESTAMP,
    _source_file            STRING,
    _batch_id               STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/staging/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS staging.users (
    user_id                 STRING,
    current_age             INT,
    age_group               STRING,
    retirement_age          INT,
    birth_year              INT,
    birth_month             INT,
    gender                  STRING,
    address                 STRING,
    latitude                DOUBLE,
    longitude               DOUBLE,
    per_capita_income       DOUBLE,
    yearly_income           DOUBLE,
    total_debt              DOUBLE,
    credit_score            INT,
    credit_score_band       STRING,
    is_invalid_credit_score BOOLEAN,
    num_credit_cards        INT,
    is_valid                BOOLEAN,
    _ingested_at            TIMESTAMP,
    _source_file            STRING,
    _batch_id               STRING
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
    account_age_months      INT,
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
    _ingested_at      TIMESTAMP,
    _source_file      STRING,
    _batch_id         STRING
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
    use_chip                INT,
    error_bad_pin               BOOLEAN,
    error_bad_cvv               BOOLEAN,
    error_bad_card_number       BOOLEAN,
    error_bad_expiration        BOOLEAN,
    error_bad_zipcode           BOOLEAN,
    error_insufficient_balance  BOOLEAN,
    error_technical_glitch      BOOLEAN,
    has_error                   BOOLEAN,
    is_fraud                    BOOLEAN,
    is_valid                    BOOLEAN,
    _batch_id                   STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/fact_transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse.dim_users (
    user_id                 STRING,
    current_age             INT,
    age_group               STRING,
    retirement_age          INT,
    gender                  STRING,
    address                 STRING,
    latitude                DOUBLE,
    longitude               DOUBLE,
    per_capita_income       DOUBLE,
    yearly_income           DOUBLE,
    total_debt              DOUBLE,
    credit_score            INT,
    credit_score_band       STRING,
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
    -- Spark SUM(amount) ghi Parquet DECIMAL; risk_score ghi DOUBLE
    amount_sum_last_1h          DECIMAL(28,2),
    amount_sum_last_24h         DECIMAL(28,2),
    amount_vs_user_avg_ratio    DECIMAL(28,2),
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
    ingested_at     TIMESTAMP,
    batch_id        STRING
)
PARTITIONED BY (event_year INT, event_month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/lake/warehouse/transaction_event_log/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
