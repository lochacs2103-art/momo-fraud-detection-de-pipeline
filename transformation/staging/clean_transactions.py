"""
clean_transactions.py — Raw → Staging: CHỈ làm sạch, KHÔNG enrich.

Nguyên tắc tách biệt trách nhiệm:
  clean_transactions.py  → làm sạch data (cast, mask, parse, flag)
  enrich_transactions.py → enrich data (join mcc, cards, fraud_labels)

Cleaning steps:
1.  Cast types (date → TIMESTAMP, mcc → INT)
2.  PCI Masking (card_number masked, cvv dropped)
3.  Amount parsing (AmountParser — 5 cols) + is_refund flag
4.  Online transaction detection (NULL merchant_state + city=ONLINE)
5.  zip cleaning (float → 5-digit string)
6.  use_chip encoding (string → INT enum 0/1/2)
7.  errors explode (1 string → 7 boolean columns + has_error)
8.  Deduplication (giữ _loaded_at mới nhất per transaction_id)
9.  is_valid flag (chưa có card_brand/is_fraud — sẽ có sau enrich)
10. Route quarantine records (amount AMBIGUOUS/INVALID)

Sau bước này: card_brand, card_type, card_number_masked, mcc_description,
is_fraud sẽ là NULL — enrich_transactions.py sẽ populate các fields đó.
"""

from pathlib import Path
from datetime import date, timedelta

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import structlog

from transformation.staging.amount_parser import apply_amount_parser

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent

# ── use_chip enum mapping ─────────────────────────────────────────────────
USE_CHIP_MAP = {
    "Swipe Transaction":  0,
    "Chip Transaction":   1,
    "Online Transaction": 2,
}

# ── error types ───────────────────────────────────────────────────────────
ERROR_TYPES = [
    ("error_bad_pin",              "Bad PIN"),
    ("error_bad_cvv",              "Bad CVV"),
    ("error_bad_card_number",      "Bad Card Number"),
    ("error_bad_expiration",       "Bad Expiration"),
    ("error_bad_zipcode",          "Bad Zipcode"),
    ("error_insufficient_balance", "Insufficient Balance"),
    ("error_technical_glitch",     "Technical Glitch"),
]


def _load_config():
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def _cast_types(df: DataFrame) -> DataFrame:
    """Step 1: Cast từ STRING (raw) sang đúng types."""
    return df \
        .withColumn("transaction_id",
                    F.col("id").cast("string")) \
        .withColumn("transaction_date",
                    F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumnRenamed("client_id", "user_id") \
        .withColumn("mcc", F.col("mcc").cast("int")) \
        .drop("id", "date")


def _pci_mask(df: DataFrame) -> DataFrame:
    """
    Step 2: PCI DSS compliance.
    card_number: mask → XXXX-XXXX-XXXX-{last4}
    cvv: DROP hoàn toàn — không lưu bất kỳ đâu
    """
    import re as _re
    mask_udf = F.udf(
        lambda cn: f"XXXX-XXXX-XXXX-{_re.sub(r'[^0-9]', '', str(cn or ''))[-4:].zfill(4)}"
        if cn else None
    )
    if "card_number" in df.columns:
        df = df.withColumn("card_number_masked", mask_udf(F.col("card_number"))) \
               .drop("card_number")
    if "cvv" in df.columns:
        df = df.drop("cvv")
    return df


def _flag_online_transactions(df: DataFrame) -> DataFrame:
    """
    Step 4: NULL merchant_state + city='ONLINE' → online transaction.
    Không phải missing data — là business meaning.
    Fill state='ONLINE', zip='ONLINE' để phân biệt với truly missing.
    """
    is_online = (
        F.col("merchant_state").isNull() &
        (F.upper(F.col("merchant_city")) == "ONLINE")
    )
    df = df.withColumn("is_online_transaction", is_online)
    df = df.withColumn("merchant_state",
        F.when(is_online, F.lit("ONLINE")).otherwise(F.col("merchant_state"))
    )
    df = df.withColumn("zip",
        F.when(is_online, F.lit("ONLINE")).otherwise(F.col("zip"))
    )
    return df


def _clean_zip(df: DataFrame) -> DataFrame:
    """
    Step 5: zip từ float string "58523.0" → "58523" (5-digit, zero-padded).
    NULL và "ONLINE" giữ nguyên.
    """
    @F.udf("string")
    def _zip_udf(z):
        if z is None or z == "ONLINE":
            return z
        try:
            # "58523.0" → 58523 → "58523"
            num = int(float(z))
            return str(num).zfill(5)
        except Exception:
            return z

    return df.withColumn("zip", _zip_udf(F.col("zip")))


def _encode_use_chip(df: DataFrame) -> DataFrame:
    """
    Step 6: Encode use_chip → INT enum.
    Giữ cột gốc để audit.
    0=SWIPE, 1=CHIP, 2=ONLINE
    """
    chip_map_expr = (
        F.when(F.col("use_chip") == "Swipe Transaction",  F.lit(0))
         .when(F.col("use_chip") == "Chip Transaction",   F.lit(1))
         .when(F.col("use_chip") == "Online Transaction", F.lit(2))
         .otherwise(F.lit(None).cast("int"))
    )
    return df \
        .withColumnRenamed("use_chip", "use_chip_raw") \
        .withColumn("use_chip", chip_map_expr)


def _explode_errors(df: DataFrame) -> DataFrame:
    """
    Step 7: Tách errors string → 7 boolean columns + has_error.
    NULL errors → tất cả False (giao dịch thành công).
    """
    for col_name, error_str in ERROR_TYPES:
        df = df.withColumn(
            col_name,
            F.when(
                F.col("errors").isNull(), F.lit(False)
            ).otherwise(
                F.col("errors").contains(error_str)
            )
        )

    # has_error = TRUE nếu bất kỳ error nào xảy ra
    error_cols = [col_name for col_name, _ in ERROR_TYPES]
    has_error_expr = F.lit(False)
    for ec in error_cols:
        has_error_expr = has_error_expr | F.col(ec)

    df = df.withColumn("has_error", has_error_expr)
    # Drop cột gốc sau khi đã explode
    df = df.drop("errors")
    return df


def _flag_refund(df: DataFrame) -> DataFrame:
    """Step 3 (sau amount parse): amount < 0 → is_refund = TRUE."""
    return df.withColumn(
        "is_refund",
        F.when(F.col("amount").isNotNull(), F.col("amount") < 0)
         .otherwise(F.lit(False))
    )


def _build_is_valid(df: DataFrame) -> DataFrame:
    """
    Step 10: is_valid flag.
    FALSE nếu: transaction_id null, transaction_date null,
               user_id null, amount AMBIGUOUS/INVALID.
    NOTE: is_refund và has_error vẫn là valid record.
    """
    return df.withColumn("is_valid",
        F.col("transaction_id").isNotNull() &
        F.col("transaction_date").isNotNull() &
        F.col("user_id").isNotNull() &
        F.col("amount").isNotNull() &
        ~F.col("amount_format").isin("AMBIGUOUS", "INVALID")
    )


def clean_transactions(
    spark: SparkSession,
    execution_date: date,
) -> dict:
    """
    Full cleaning pipeline cho 1 ngày.
    Airflow truyền execution_date vào → không bao giờ process cả năm 1 lúc.
    """
    cfg = _load_config()
    year, month, day = execution_date.year, execution_date.month, execution_date.day

    raw_path        = f"{cfg['tables']['transactions']['raw']}/year={year}/month={month}/day={day}"
    staging_path    = cfg["tables"]["transactions"]["staging"]
    quarantine_path = cfg["tables"]["transactions"]["quarantine"]

    logger.info("clean_transactions.start", date=execution_date.isoformat())

    # ── Đọc raw partition của ngày này ──────────────────────────────────
    df = spark.read.parquet(raw_path)
    raw_count = df.count()

    # ── Apply cleaning steps (KHÔNG enrich — enrich_transactions.py làm sau) ─
    df = _cast_types(df)
    df = _pci_mask(df)
    df = apply_amount_parser(df, raw_col="amount")
    df = _flag_refund(df)
    df = _flag_online_transactions(df)
    df = _clean_zip(df)
    df = _encode_use_chip(df)
    df = _explode_errors(df)

    # ── Dedup: giữ _loaded_at mới nhất per transaction_id ────────────────
    w = Window.partitionBy("transaction_id").orderBy(F.col("_loaded_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    # ── is_valid flag ────────────────────────────────────────────────────
    df = _build_is_valid(df)

    # ── Split valid vs quarantine ─────────────────────────────────────────
    df_valid = df.filter(F.col("is_valid") == True)
    df_quarantine = df.filter(F.col("is_valid") == False).select(
        "transaction_id", "amount_raw", "amount_format", "amount_parse_note",
        F.col("amount_format").alias("quarantine_reason"),
        F.current_timestamp().alias("quarantine_ts"),
        F.lit(None).cast("string").alias("resolved_by"),
        F.lit(None).cast("timestamp").alias("resolved_at"),
        F.lit(False).alias("is_resolved"),
        "_batch_id",
        F.lit(year).alias("year"),
        F.lit(month).alias("month"),
        F.lit(day).alias("day"),
    )

    # ── Write ─────────────────────────────────────────────────────────────
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_valid \
        .withColumn("year",  F.lit(year)) \
        .withColumn("month", F.lit(month)) \
        .withColumn("day",   F.lit(day)) \
        .repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(staging_path)

    if df_quarantine.count() > 0:
        # Dùng overwrite dynamic partition — idempotent như valid data
        # Chạy lại cùng ngày → overwrite đúng partition đó, không duplicate
        df_quarantine.write.mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("year", "month", "day") \
            .parquet(quarantine_path)

    valid_count      = df_valid.count()
    quarantine_count = df_quarantine.count()

    logger.info("clean_transactions.done",
                date=execution_date.isoformat(),
                raw=raw_count,
                valid=valid_count,
                quarantine=quarantine_count)

    return {
        "date":            execution_date.isoformat(),
        "raw_count":       raw_count,
        "valid_count":     valid_count,
        "quarantine_count": quarantine_count,
    }


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session
    import sys

    spark = get_spark_session("clean_transactions")
    try:
        exec_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 \
                    else date.today() - timedelta(days=1)
        result = clean_transactions(spark, exec_date)
        print(result)
    finally:
        stop_spark_session(spark)
