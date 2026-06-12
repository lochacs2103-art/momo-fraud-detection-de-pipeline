"""
clean_mcc.py — Clean raw mcc_codes → staging.
Cast mcc_code STRING → mcc INT, rename description → mcc_description.
"""

import os
from pathlib import Path
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import structlog

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent.parent))
logger = structlog.get_logger(__name__)


def clean_mcc(spark: SparkSession) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    raw_path     = cfg["tables"]["mcc_codes"]["raw"]
    staging_path = cfg["tables"]["mcc_codes"]["staging"]

    logger.info("clean_mcc.start")

    df = spark.read.parquet(raw_path) \
        .withColumn("mcc", F.col("mcc_code").cast("int")) \
        .withColumnRenamed("description", "mcc_description") \
        .dropDuplicates(["mcc"]) \
        .filter(F.col("mcc").isNotNull())

    df.coalesce(1) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .parquet(staging_path)

    count = df.count()
    logger.info("clean_mcc.done", count=count)
    return {"count": count}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session
    spark = get_spark_session("clean_mcc")
    try:
        result = clean_mcc(spark)
        print(f"=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
