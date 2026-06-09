"""
enrich_transactions.py — Enrich staging transactions với dimension tables.

Chạy SAU clean_transactions.py. Đọc từ staging (đã clean), join thêm:
  1. mcc_codes   → mcc_description  ('UNKNOWN' nếu không khớp, NULL nếu mcc null)
  2. cards       → card_brand, card_type, card_number_masked, card_on_dark_web
  3. fraud_labels → is_fraud (TRUE/FALSE/NULL nếu chưa có label)

Tất cả đều LEFT JOIN — transaction vẫn hợp lệ dù không match dim record.
Tất cả dim tables nhỏ → dùng broadcast() để tránh shuffle.

Sau bước này staging.transactions mới có đủ schema như Hive DDL định nghĩa.
"""

from pathlib import Path
from datetime import date, timedelta

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import structlog

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent


def _load_config():
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def enrich_transactions(spark: SparkSession, execution_date: date) -> dict:
    """
    Enrich staging transactions của 1 ngày với mcc, cards, fraud_labels.
    Ghi đè lại đúng partition đó (idempotent).
    """
    cfg = _load_config()
    year, month, day = execution_date.year, execution_date.month, execution_date.day

    # Đọc từ staging partition đã được clean
    staging_partition = (
        f"{cfg['tables']['transactions']['staging']}"
        f"/year={year}/month={month}/day={day}"
    )
    staging_path  = cfg["tables"]["transactions"]["staging"]
    mcc_path      = cfg["tables"]["mcc_codes"]["staging"]   # staging có mcc INT + mcc_description
    cards_path    = cfg["tables"]["cards"]["raw"]
    fraud_path    = cfg["lake"]["raw"] + "/fraud_labels"

    logger.info("enrich_transactions.start", date=execution_date.isoformat())

    # ── Đọc staging partition đã clean ───────────────────────────────────
    df = spark.read.parquet(staging_partition)
    count_before = df.count()

    # ── Load dimension tables (broadcast — tất cả nhỏ hơn staging) ───────

    # 1. mcc_codes — ~300 rows
    mcc_df = spark.read.parquet(mcc_path) \
        .select("mcc", "mcc_description") \
        .dropDuplicates(["mcc"])

    # 2. cards — lấy đúng columns cần, drop cvv/card_number đã được mask ở clean
    cards_df = spark.read.parquet(cards_path) \
        .withColumnRenamed("id", "card_id") \
        .select("card_id", "card_brand", "card_type", "card_on_dark_web") \
        .dropDuplicates(["card_id"])

    # 3. fraud_labels — "Yes"/"No" → boolean
    fraud_df = spark.read.parquet(fraud_path) \
        .select(
            F.col("transaction_id"),
            (F.upper(F.col("is_fraud")) == "YES").cast("boolean").alias("is_fraud")
        ) \
        .dropDuplicates(["transaction_id"])

    # ── Broadcast joins ───────────────────────────────────────────────────

    # Join mcc → mcc_description
    df = df.join(F.broadcast(mcc_df), on="mcc", how="left")
    df = df.withColumn(
        "mcc_description",
        F.when(F.col("mcc").isNull(),              F.lit(None).cast("string"))
         .when(F.col("mcc_description").isNull(),  F.lit("UNKNOWN"))
         .otherwise(F.col("mcc_description"))
    )

    # Join cards → card_brand, card_type, card_on_dark_web
    # card_number_masked đã có từ clean_transactions (PCI mask)
    df = df.join(F.broadcast(cards_df), on="card_id", how="left")

    # Join fraud labels → is_fraud
    # Drop cột is_fraud cũ (NULL từ clean step) trước khi join
    if "is_fraud" in df.columns:
        df = df.drop("is_fraud")
    df = df.join(F.broadcast(fraud_df), on="transaction_id", how="left")
    # is_fraud NULL = chưa có label (late arriving) — hợp lệ, không set False

    # ── Ghi lại đúng partition (overwrite idempotent) ─────────────────────
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.withColumn("year",  F.lit(year)) \
      .withColumn("month", F.lit(month)) \
      .withColumn("day",   F.lit(day)) \
      .repartition(F.col("year"), F.col("month"), F.col("day")) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("year", "month", "day") \
      .parquet(staging_path)

    count_after = df.count()
    logger.info("enrich_transactions.done",
                date=execution_date.isoformat(),
                before=count_before,
                after=count_after)

    return {
        "date":         execution_date.isoformat(),
        "count_before": count_before,
        "count_after":  count_after,
    }


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session
    import sys

    spark = get_spark_session("enrich_transactions")
    try:
        exec_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 \
                    else date.today() - timedelta(days=1)
        result = enrich_transactions(spark, exec_date)
        print(result)
    finally:
        stop_spark_session(spark)
