"""Schema definition cho users_data.csv"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, DoubleType, BooleanType
)

RAW_CSV_SCHEMA = StructType([
    StructField("id",               StringType(), nullable=True),
    StructField("current_age",      StringType(), nullable=True),
    StructField("retirement_age",   StringType(), nullable=True),
    StructField("birth_year",       StringType(), nullable=True),
    StructField("birth_month",      StringType(), nullable=True),
    StructField("gender",           StringType(), nullable=True),
    StructField("address",          StringType(), nullable=True),
    StructField("latitude",         StringType(), nullable=True),
    StructField("longitude",        StringType(), nullable=True),
    StructField("per_capita_income",StringType(), nullable=True),  # có thể có "$" prefix
    StructField("yearly_income",    StringType(), nullable=True),  # có thể có "$" prefix
    StructField("total_debt",       StringType(), nullable=True),  # có thể có "$" prefix
    StructField("credit_score",     StringType(), nullable=True),
    StructField("num_credit_cards", StringType(), nullable=True),
])

STAGING_SCHEMA = StructType([
    StructField("user_id",                  StringType(),   nullable=False),
    StructField("current_age",              IntegerType(),  nullable=True),
    StructField("age_group",                StringType(),   nullable=True),  # TEEN|YOUNG_ADULT|ADULT|MIDDLE_AGED|SENIOR
    StructField("retirement_age",           IntegerType(),  nullable=True),
    StructField("birth_year",               IntegerType(),  nullable=True),
    StructField("birth_month",              IntegerType(),  nullable=True),
    StructField("gender",                   StringType(),   nullable=True),
    StructField("address",                  StringType(),   nullable=True),
    StructField("latitude",                 DoubleType(),   nullable=True),
    StructField("longitude",                DoubleType(),   nullable=True),
    StructField("per_capita_income",        DoubleType(),   nullable=True),
    StructField("yearly_income",            DoubleType(),   nullable=True),
    StructField("total_debt",               DoubleType(),   nullable=True),
    StructField("credit_score",             IntegerType(),  nullable=True),
    StructField("credit_score_band",        StringType(),   nullable=True),  # POOR|FAIR|GOOD|VERY_GOOD|EXCEPTIONAL|INVALID
    StructField("is_invalid_credit_score",  BooleanType(),  nullable=True),
    StructField("num_credit_cards",         IntegerType(),  nullable=True),

    # Metadata
    StructField("_ingested_at",      TimestampType(), nullable=False),
    StructField("_source_file",      StringType(),   nullable=False),
    StructField("_batch_id",         StringType(),   nullable=False),

    # Partition
    StructField("created_year",      IntegerType(),  nullable=False),
    StructField("created_month",     IntegerType(),  nullable=False),
])

PARTITION_COLS = ["created_year", "created_month"]
