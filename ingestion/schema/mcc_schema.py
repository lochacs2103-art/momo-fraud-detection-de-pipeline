"""Schema definition cho mcc_codes.json"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)

# MCC JSON có dạng: {"0742": "Veterinary Services", "1711": "Plumbing", ...}
# Sau khi explode: mcc_code (string key) + description (string value)
RAW_JSON_SCHEMA = StructType([
    StructField("mcc_code",     StringType(), nullable=False),
    StructField("description",  StringType(), nullable=True),
])

STAGING_SCHEMA = StructType([
    StructField("mcc",              IntegerType(), nullable=False),  # cast từ string
    StructField("mcc_code",         StringType(),  nullable=False),  # giữ nguyên string key
    StructField("mcc_description",  StringType(),  nullable=True),

    # Metadata
    StructField("_ingested_at",     TimestampType(), nullable=False),
    StructField("_source_file",     StringType(),  nullable=False),
    StructField("_batch_id",        StringType(),  nullable=False),
])

# Không có partition — static table, ~300 rows
PARTITION_COLS = []
