"""
HDFSCompactor — merge nhiều small files thành ít large files.

Chạy sau ingestion mỗi đêm.
Target: 128MB per file (= 1 HDFS block).
Dedup trong quá trình compact luôn.
Dùng dynamic partition overwrite → atomic, không ảnh hưởng partitions khác.
"""

from pathlib import Path
from datetime import date, timedelta
from typing import Optional

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import structlog

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent

TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128MB
BYTES_PER_ROW_ESTIMATE = 500               # ~500 bytes per row after snappy


class HDFSCompactor:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    def _target_files(self, row_count: int) -> int:
        estimated = row_count * BYTES_PER_ROW_ESTIMATE
        return max(1, estimated // TARGET_FILE_SIZE_BYTES)

    def compact_day(self, table: str, hdfs_path: str,
                    year: int, month: int, day: int,
                    dedup_key: Optional[str] = None) -> dict:
        """Compact 1 daily partition."""
        partition_path = f"{hdfs_path}/year={year}/month={month}/day={day}"

        try:
            df = self.spark.read.parquet(partition_path)
        except Exception:
            logger.warning("compactor.partition_not_found", path=partition_path)
            return {"skipped": True, "path": partition_path}

        if dedup_key:
            before = df.count()
            df = df.dropDuplicates([dedup_key])
            after = df.count()
            dupes = before - after
            if dupes > 0:
                logger.info("compactor.dedup", table=table, dupes=dupes)
        else:
            after = df.count()

        # Sort within partition để cải thiện predicate pushdown
        df = df.sortWithinPartitions("user_id") if "user_id" in df.columns \
             else df

        target = self._target_files(after)

        df.coalesce(target) \
          .write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(partition_path)

        logger.info("compactor.day_done",
                    table=table,
                    partition=f"{year}/{month}/{day}",
                    rows=after,
                    files=target)

        return {
            "table": table,
            "partition": f"year={year}/month={month}/day={day}",
            "row_count": after,
            "target_files": target,
        }

    def compact_yesterday(self, table: str, hdfs_path: str,
                          dedup_key: Optional[str] = None) -> dict:
        yesterday = date.today() - timedelta(days=1)
        return self.compact_day(
            table, hdfs_path,
            yesterday.year, yesterday.month, yesterday.day,
            dedup_key
        )

    def compact_month(self, table: str, hdfs_path: str,
                      year: int, month: int,
                      dedup_key: Optional[str] = None,
                      target_compression: str = "zstd") -> dict:
        """
        Monthly compaction: merge 30 daily partitions → 1 monthly partition.
        Output path: warm/ với zstd compression.
        Sau khi verify → có thể xóa daily partitions.
        """
        with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
            cfg = yaml.safe_load(f)

        monthly_path = f"{cfg['lake']['warm']}/{table}/year={year}/month={month}"

        # Đọc toàn bộ tháng từ hot layer
        monthly_data_path = f"{hdfs_path}/year={year}/month={month}"
        try:
            df = self.spark.read.parquet(monthly_data_path)
        except Exception:
            logger.warning("compactor.month_not_found", path=monthly_data_path)
            return {"skipped": True}

        if dedup_key:
            df = df.dropDuplicates([dedup_key])

        row_count = df.count()
        # Warm tier: 256MB files (double size, ít files hơn)
        target = max(1, (row_count * BYTES_PER_ROW_ESTIMATE) // (256 * 1024 * 1024))

        df.coalesce(target) \
          .write \
          .mode("overwrite") \
          .option("compression", target_compression) \
          .parquet(monthly_path)

        logger.info("compactor.month_done",
                    table=table,
                    year=year, month=month,
                    row_count=row_count,
                    target_files=target,
                    output=monthly_path)

        return {
            "table": table,
            "year": year, "month": month,
            "row_count": row_count,
            "target_files": target,
            "output_path": monthly_path,
        }


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session
    import sys, yaml

    spark = get_spark_session("compaction")
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        hdfs_cfg = yaml.safe_load(f)

    compactor = HDFSCompactor(spark)
    try:
        # Daily compaction: transactions của hôm qua
        result = compactor.compact_yesterday(
            table="transactions",
            hdfs_path=hdfs_cfg["tables"]["transactions"]["raw"],
            dedup_key="id"
        )
        print(f"Compaction result: {result}")
    finally:
        stop_spark_session(spark)
