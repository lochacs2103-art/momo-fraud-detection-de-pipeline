"""
enrich_transactions_full.py — Enrich toàn bộ staging transactions trong 1 job.
"""

import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))
logger = structlog.get_logger(__name__)


def enrich_transactions_full(spark: SparkSession) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path = cfg["tables"]["transactions"]["staging"]
    mcc_path     = cfg["tables"]["mcc_codes"]["staging"]
    cards_path   = cfg["tables"]["cards"]["staging"]
    fraud_path   = cfg["lake"]["raw"] + "/fraud_labels"

    logger.info("enrich_transactions_full.start")

    # Tăng shuffle partitions để giảm memory per partition
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    df = spark.read.parquet(staging_path)

    # MCC — ~109 rows, broadcast hoàn toàn
    mcc_df = spark.read.parquet(mcc_path) \
        .select("mcc", "mcc_description") \
        .dropDuplicates(["mcc"])

    # Cards — ~6K rows, broadcast
    cards_df = spark.read.parquet(cards_path)
    if "id" in cards_df.columns:
        cards_df = cards_df.withColumnRenamed("id", "card_id")
    cards_df = cards_df.select("card_id", "card_brand", "card_type", "card_on_dark_web") \
        .dropDuplicates(["card_id"])

    # Fraud labels — 8.9M rows, KHÔNG broadcast, dùng sort-merge join
    fraud_df = spark.read.parquet(fraud_path) \
        .select(
            F.col("transaction_id"),
            (F.upper(F.col("is_fraud")) == "YES").cast("boolean").alias("is_fraud")
        ).dropDuplicates(["transaction_id"])

    # Drop existing enriched columns để tránh duplicate
    cols_to_drop = [c for c in ["mcc_description", "card_brand", "card_type",
                                 "card_on_dark_web", "is_fraud"] if c in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    # Broadcast joins cho bảng nhỏ
    df = df.join(F.broadcast(mcc_df), on="mcc", how="left")
    df = df.withColumn(
        "mcc_description",
        F.when(F.col("mcc").isNull(), F.lit(None).cast("string"))
         .when(F.col("mcc_description").isNull(), F.lit("UNKNOWN"))
         .otherwise(F.col("mcc_description"))
    )
    df = df.join(F.broadcast(cards_df), on="card_id", how="left")

    # Sort-merge join cho fraud labels (lớn)
    df = df.join(fraud_df, on="transaction_id", how="left")

    # Checkpoint để break long lineage chain → tránh StackOverflowError
    spark.sparkContext.setCheckpointDir("hdfs://namenode:9000/tmp/spark-checkpoint")
    df = df.checkpoint()

    # Write
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.repartition(F.col("year"), F.col("month"), F.col("day")) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("year", "month", "day") \
      .parquet(staging_path)

    logger.info("enrich_transactions_full.done")
    return {"status": "done"}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session
    spark = get_spark_session("enrich_transactions_full")
    try:
        result = enrich_transactions_full(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
