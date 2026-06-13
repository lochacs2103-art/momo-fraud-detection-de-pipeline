"""
clean_transactions_full.py — Xử lý TOÀN BỘ transactions trong 1 Spark job.

Thay vì loop 120 tháng × 1 job/tháng = 120 job startups (~30 phút overhead),
job này đọc tất cả partitions cùng lúc, Spark tự parallel hóa.

Dùng cho backfill lần đầu (historical data).
Production daily incremental vẫn dùng clean_transactions.py cho 1 ngày.
"""

import os
from pathlib import Path
from datetime import date

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))

# Import các helper functions từ clean_transactions
import sys
sys.path.insert(0, str(PROJECT_ROOT))
from transformation.staging.clean_transactions import (
    _cast_types, _pci_mask, _flag_online_transactions,
    _clean_zip, _encode_use_chip, _explode_errors,
    _flag_refund, _build_is_valid
)
from transformation.staging.amount_parser import apply_amount_parser

logger = structlog.get_logger(__name__)


def clean_transactions_full(spark: SparkSession) -> dict:
    """
    Clean toàn bộ raw transactions → staging trong 1 job.
    Spark tự chia parallel tasks theo partitions.
    """
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    raw_path       = cfg["tables"]["transactions"]["raw"]
    staging_path   = cfg["tables"]["transactions"]["staging"]
    quarantine_path = cfg["tables"]["transactions"]["quarantine"]
    mcc_raw_path   = cfg["tables"]["mcc_codes"]["raw"]

    logger.info("clean_transactions_full.start", raw_path=raw_path)

    # Đọc TẤT CẢ partitions cùng lúc
    # Spark tự parallel hóa theo year/month/day partitions
    df = spark.read.parquet(raw_path)
    raw_count = df.count()
    logger.info("clean_transactions_full.read_done", raw_count=raw_count)

    # mcc_codes để enrich
    mcc_df = spark.read.parquet(mcc_raw_path) \
        .select("mcc_code", "description") \
        .withColumnRenamed("mcc_code", "mcc_str") \
        .withColumn("mcc", F.col("mcc_str").cast("int")) \
        .withColumnRenamed("description", "mcc_description") \
        .select("mcc", "mcc_description") \
        .dropDuplicates(["mcc"])

    # Apply cleaning steps
    df = _cast_types(df)
    df = _pci_mask(df)
    df = apply_amount_parser(df, raw_col="amount")
    df = _flag_refund(df)
    df = _flag_online_transactions(df)
    df = _clean_zip(df)
    df = _encode_use_chip(df)
    df = _explode_errors(df)

    # Enrich mcc
    df = df.join(F.broadcast(mcc_df), on="mcc", how="left")
    df = df.withColumn(
        "mcc_description",
        F.when(F.col("mcc").isNull(), F.lit(None).cast("string"))
         .when(F.col("mcc_description").isNull(), F.lit("UNKNOWN"))
         .otherwise(F.col("mcc_description"))
    )

    # Dedup
    w = Window.partitionBy("transaction_id").orderBy(F.col("_loaded_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    # is_valid
    df = _build_is_valid(df)

    # Split valid vs quarantine
    df_valid      = df.filter(F.col("is_valid") == True)
    df_quarantine = df.filter(F.col("is_valid") == False).select(
        "transaction_id", "amount_raw", "amount_format", "amount_parse_note",
        F.col("amount_format").alias("quarantine_reason"),
        F.current_timestamp().alias("quarantine_ts"),
        F.lit(None).cast("string").alias("resolved_by"),
        F.lit(None).cast("timestamp").alias("resolved_at"),
        F.lit(False).alias("is_resolved"),
        "_batch_id",
        "year", "month", "day"
    )

    # Write staging — repartition by year/month/day
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # count() TRƯỚC write() — tránh re-execute lineage 13.3M rows lần 2
    valid_count      = df_valid.count()
    quarantine_count = df_quarantine.count()

    df_valid \
        .repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(staging_path)

    if quarantine_count > 0:
        df_quarantine.write.mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("year", "month", "day") \
            .parquet(quarantine_path)

    logger.info("clean_transactions_full.done",
                raw=raw_count, valid=valid_count, quarantine=quarantine_count)

    return {"raw": raw_count, "valid": valid_count, "quarantine": quarantine_count}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("clean_transactions_full")
    try:
        result = clean_transactions_full(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
