"""
Schema definition cho transactions_data.csv

Tại sao define schema explicit?
- Tránh Spark đọc toàn bộ file để infer → tốn thời gian với file lớn
- Type safety: amount là Double, không phải String
- Contract: nếu source gửi sai column → fail sớm, rõ ràng
- amount_raw giữ nguyên raw string để audit — xem ENGINEERING_LOG section 5
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    TimestampType, BooleanType, DecimalType
)

# Schema khi đọc từ CSV — tất cả đều là StringType
# Lý do: CSV không có type info, đọc vào String trước, cast sau trong staging
# Nếu đọc thẳng vào Double mà gặp "$1,234" → exception ngay
RAW_CSV_SCHEMA = StructType([
    StructField("id",                StringType(), nullable=True),
    StructField("date",              StringType(), nullable=True),   # "2023-06-01 10:00:00"
    StructField("client_id",         StringType(), nullable=True),
    StructField("card_id",           StringType(), nullable=True),
    StructField("amount",            StringType(), nullable=True),   # raw: "$1,234.56"
    StructField("use_chip",          StringType(), nullable=True),   # "Chip Transaction" / "Swipe"
    StructField("merchant_id",       StringType(), nullable=True),
    StructField("merchant_city",     StringType(), nullable=True),
    StructField("merchant_state",    StringType(), nullable=True),
    StructField("zip",               StringType(), nullable=True),
    StructField("mcc",               StringType(), nullable=True),   # merchant category code
    StructField("errors",            StringType(), nullable=True),   # comma-separated error codes
])

# Schema sau khi clean và cast trong staging layer
# Đây là contract downstream consumers (dbt, Trino queries) có thể rely on
STAGING_SCHEMA = StructType([
    StructField("transaction_id",     StringType(),   nullable=False),
    StructField("transaction_date",   TimestampType(), nullable=True),
    StructField("user_id",            StringType(),   nullable=True),
    StructField("card_id",            StringType(),   nullable=True),

    # Amount — 5 cột từ AmountParser
    # DECIMAL(18,2) thay vì DOUBLE — tránh floating-point precision error
    # Fintech yêu cầu: 10.10 là 10.10, không phải 10.09999999...
    StructField("amount_raw",         StringType(),      nullable=True),
    StructField("amount",             DecimalType(18,2), nullable=True),
    StructField("amount_currency",    StringType(),      nullable=True),
    StructField("amount_format",      StringType(),      nullable=True),
    StructField("amount_parse_note",  StringType(),      nullable=True),

    # Amount flag
    StructField("is_refund",          BooleanType(),  nullable=True),

    # use_chip: encoded INT + raw string
    StructField("use_chip",           IntegerType(),  nullable=True),   # 0=SWIPE,1=CHIP,2=ONLINE
    StructField("use_chip_raw",       StringType(),   nullable=True),

    # Online transaction
    StructField("is_online_transaction", BooleanType(), nullable=True),

    # Merchant
    StructField("merchant_id",        StringType(),   nullable=True),
    StructField("merchant_city",      StringType(),   nullable=True),
    StructField("merchant_state",     StringType(),   nullable=True),
    StructField("zip",                StringType(),   nullable=True),
    StructField("mcc",                IntegerType(),  nullable=True),
    StructField("mcc_description",    StringType(),   nullable=True),

    # Card info
    StructField("card_brand",         StringType(),   nullable=True),
    StructField("card_type",          StringType(),   nullable=True),
    StructField("card_number_masked", StringType(),   nullable=True),

    # Error flags (exploded từ errors string)
    StructField("error_bad_pin",               BooleanType(), nullable=True),
    StructField("error_bad_cvv",               BooleanType(), nullable=True),
    StructField("error_bad_card_number",       BooleanType(), nullable=True),
    StructField("error_bad_expiration",        BooleanType(), nullable=True),
    StructField("error_bad_zipcode",           BooleanType(), nullable=True),
    StructField("error_insufficient_balance",  BooleanType(), nullable=True),
    StructField("error_technical_glitch",      BooleanType(), nullable=True),
    StructField("has_error",                   BooleanType(), nullable=True),

    # Fraud label
    StructField("is_fraud",           BooleanType(),  nullable=True),

    # Quality flag
    StructField("is_valid",           BooleanType(),  nullable=False),

    # Metadata
    StructField("_ingested_at",       TimestampType(), nullable=False),
    StructField("_source_file",       StringType(),   nullable=False),
    StructField("_batch_id",          StringType(),   nullable=False),

    # Partition
    StructField("year",               IntegerType(),  nullable=False),
    StructField("month",              IntegerType(),  nullable=False),
    StructField("day",                IntegerType(),  nullable=False),
])

# Partition columns — dùng trong .partitionBy() khi write
PARTITION_COLS = ["year", "month", "day"]
