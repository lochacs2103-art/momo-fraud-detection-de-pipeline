"""
enrich_transactions_full.py
Enrich theo năm để tránh OOM và StackOverflow với 13.3M rows.
"""

import os
from pathlib import Path
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))
logger = structlog.get_logger(__name__)


def enrich_year(spark, df, mcc_df, cards_df, year, staging_path):
    """Enrich 1 năm với mcc + cards. Fraud labels join ở warehouse layer."""
    df_year = df.filter(F.col("year") == year)

    cols_to_drop = [c for c in ["mcc_description", "card_brand", "card_type",
                                 "card_on_dark_web"] if c in df_year.columns]
    if cols_to_drop:
        df_year = df_year.drop(*cols_to_drop)

    # Broadcast joins — cả 2 đều nhỏ, không gây OOM
    df_year = df_year.join(F.broadcast(mcc_df), on="mcc", how="left")
    df_year = df_year.withColumn(
        "mcc_description",
        F.when(F.col("mcc").isNull(), F.lit(None).cast("string"))
         .when(F.col("mcc_description").isNull(), F.lit("UNKNOWN"))
         .otherwise(F.col("mcc_description"))
    )
    df_year = df_year.join(F.broadcast(cards_df), on="card_id", how="left")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df_year \
        .repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(staging_path)

    count = df_year.count()
    logger.info("enrich_year.done", year=year, count=count)
    return count


def enrich_transactions_full(spark: SparkSession) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path = cfg["tables"]["transactions"]["staging"]
    mcc_path     = cfg["tables"]["mcc_codes"]["staging"]
    cards_path   = cfg["tables"]["cards"]["staging"]

    spark.conf.set("spark.sql.shuffle.partitions", "50")
    logger.info("enrich_transactions_full.start")

    mcc_df = spark.read.parquet(mcc_path) \
        .select("mcc", "mcc_description").dropDuplicates(["mcc"])

    cards_df = spark.read.parquet(cards_path)
    if "id" in cards_df.columns:
        cards_df = cards_df.withColumnRenamed("id", "card_id")
    cards_df = cards_df.select("card_id", "card_brand", "card_type", "card_on_dark_web") \
        .dropDuplicates(["card_id"])

    mcc_df.cache(); mcc_df.count()
    cards_df.cache(); cards_df.count()

    df = spark.read.parquet(staging_path)

    total = 0
    for year in [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]:
        logger.info("enrich_year.start", year=year)
        count = enrich_year(spark, df, mcc_df, cards_df, year, staging_path)
        total += count
        logger.info("progress", year=year, done=count, total_so_far=total)

    mcc_df.unpersist()
    cards_df.unpersist()

    logger.info("enrich_transactions_full.done", total=total)
    return {"total": total}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session
    spark = get_spark_session("enrich_transactions_full")
    try:
        result = enrich_transactions_full(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
