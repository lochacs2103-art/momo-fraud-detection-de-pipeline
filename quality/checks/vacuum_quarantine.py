"""
vacuum_quarantine.py — Xóa quarantine records đã resolved quá N ngày.
Chạy bởi Airflow compaction DAG.
"""

import argparse
from pathlib import Path
from datetime import date, timedelta

import yaml
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).parent.parent.parent


def vacuum_quarantine(retention_days: int = 30):
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("vacuum_quarantine")
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    quarantine_path = cfg["tables"]["transactions"]["quarantine"]
    cutoff = date.today() - timedelta(days=retention_days)

    try:
        df = spark.read.parquet(quarantine_path)

        # Giữ lại: chưa resolved HOẶC mới resolved (< cutoff)
        df_keep = df.filter(
            (F.col("is_resolved") == False) |
            (F.col("resolved_at") >= F.lit(cutoff.isoformat()).cast("timestamp"))
        )

        before = df.count()
        after  = df_keep.count()
        removed = before - after

        # Overwrite
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df_keep.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("year", "month", "day") \
            .parquet(quarantine_path)

        print(f"Vacuum done: removed {removed} resolved records older than {cutoff}")
        return {"removed": removed, "remaining": after}

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--retention-days", type=int, default=30)
    args = parser.parse_args()
    vacuum_quarantine(args.retention_days)
