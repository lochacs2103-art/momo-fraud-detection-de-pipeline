"""Schema definition cho cards_data.csv"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, BooleanType, DoubleType
)

RAW_CSV_SCHEMA = StructType([
    StructField("id",               StringType(), nullable=True),
    StructField("client_id",        StringType(), nullable=True),
    StructField("card_brand",       StringType(), nullable=True),   # Visa, Mastercard, ...
    StructField("card_type",        StringType(), nullable=True),   # Credit, Debit, ...
    StructField("card_number",      StringType(), nullable=True),   # PAN — sensitive!
    StructField("expires",          StringType(), nullable=True),   # "MM/YYYY"
    StructField("cvv",              StringType(), nullable=True),   # sensitive!
    StructField("has_chip",         StringType(), nullable=True),   # "YES"/"NO"
    StructField("num_cards_issued", StringType(), nullable=True),
    StructField("credit_limit",     StringType(), nullable=True),   # có thể có "$" prefix
    StructField("acct_open_date",   StringType(), nullable=True),
    StructField("year_pin_last_changed", StringType(), nullable=True),
    StructField("card_on_dark_web", StringType(), nullable=True),   # "Yes"/"No"
])

STAGING_SCHEMA = StructType([
    StructField("card_id",              StringType(),   nullable=False),
    StructField("user_id",              StringType(),   nullable=True),
    StructField("card_brand",           StringType(),   nullable=True),
    StructField("card_type",            StringType(),   nullable=True),
    StructField("card_number_masked",   StringType(),   nullable=True),  # XXXX-XXXX-XXXX-1234
    StructField("expires_month",        IntegerType(),  nullable=True),
    StructField("expires_year",         IntegerType(),  nullable=True),
    StructField("has_chip",             BooleanType(),  nullable=True),
    StructField("num_cards_issued",     IntegerType(),  nullable=True),
    StructField("credit_limit",         DoubleType(),   nullable=True),
    StructField("acct_open_date",       StringType(),   nullable=True),
    StructField("account_age_months",   IntegerType(),  nullable=True),  # tính từ acct_open_date
    StructField("year_pin_last_changed",IntegerType(),  nullable=True),
    StructField("card_on_dark_web",     BooleanType(),  nullable=True),

    # Metadata
    StructField("_ingested_at",         TimestampType(), nullable=False),
    StructField("_source_file",         StringType(),   nullable=False),
    StructField("_batch_id",            StringType(),   nullable=False),

    # Partition
    StructField("card_brand_part",      StringType(),   nullable=False),
    StructField("expires_year_part",    IntegerType(),  nullable=False),
])

PARTITION_COLS = ["card_brand_part", "expires_year_part"]
