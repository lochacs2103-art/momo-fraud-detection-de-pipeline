"""
enrich_transactions.py — Join staging transactions với dimension tables.

Enrichments:
- mcc_codes   → thêm mcc_description  (broadcast, ~300 rows)
- cards       → thêm card_brand, card_type, card_on_dark_web  (broadcast nếu đủ nhỏ)
- fraud_labels → thêm is_fraud  (broadcast)

Tất cả đều LEFT JOIN — transaction vẫn hợp lệ dù không có dim record.
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


def enrich_transactions(
    spark: SparkSession,
    execution_date: date,
) -> dict:
    """
    Enrich staging transactions với dims.
    Chạy sau clean_transactions.
    """
    cfg = _load_config()
    year, month, day = execution_date.year, execution_date.month, execution_date.day

    staging_path  = cfg["tables"]["transactions"]["staging"]
    mcc_path      = cfg["tables"]["mcc_codes"]["staging"]
    cards_path    = cfg["tables"]["cards"]["staging"]

    # Fraud labels nằm ở raw layer (không transform gì thêm)
    fraud_path    = cfg["lake"]["raw"] + "/fraud_labels"

    logger.info("enrich_transactions.start", date=execution_date.isoformat())

    # ── Đọc staging transactions của ngày này ─────────────────────────────
    txn_path = f"{staging_path}/year={year}/month={month}/day={day}"
    df = spark.read.parquet(txn_path)

    # ── Load dimension tables ─────────────────────────────────────────────
    # mcc_codes: ~300 rows → broadcast hoàn toàn
    mcc_df = spark.read.parquet(mcc_path) \
        .select("mcc", "mcc_description") \
        .dropDuplicates(["mcc"])

    # cards: chỉ lấy columns cần thiết → giảm broadcast size
    cards_df = spark.read.parquet(cards_path) \
        .select("card_id", "card_brand", "card_type", "card_on_dark_web") \
        .dropDuplicates(["card_id"])

    # fraud_labels: "Yes"/"No" → cast sang boolean
    fraud_df = spark.read.parquet(fraud_path) \
        .select(
            F.col("transaction_id"),
            (F.upper(F.col("is_fraud")) == "YES").cast("boolean").alias("is_fraud")
        ) \
        .dropDuplicates(["transaction_id"])

    # ── Broadcast joins ───────────────────────────────────────────────────
    # Broadcast: copy toàn bộ dim lên mỗi executor → không shuffle transactions
    df = df.join(F.broadcast(mcc_df),   on="mcc",            how="left")
    df = df.join(F.broadcast(cards_df), on="card_id",        how="left")
    df = df.join(F.broadcast(fraud_df), on="transaction_id", how="left")

    # is_fraud NULL → chưa có label (late arriving) → để NULL, không set False
    # downstream dbt model sẽ handle

    # ── Overwrite partition đã enrich ─────────────────────────────────────
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.withColumn("year",  F.lit(year)) \
      .withColumn("month", F.lit(month)) \
      .withColumn("day",   F.lit(day)) \
      .repartition(F.col("year"), F.col("month"), F.col("day")) \
      .write \
      .mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("year", "month", "day") \
      .parquet(staging_path)

    row_count = df.count()
    logger.info("enrich_transactions.done",
                date=execution_date.isoformat(),
                row_count=row_count)

    return {"date": execution_date.isoformat(), "row_count": row_count}


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
