"""
build_dim_cards.py — SCD Type 1 cho dimension cards.

SCD Type 1: Overwrite khi có thay đổi. Không giữ lịch sử.
Tại sao cards dùng Type 1 thay vì Type 2?
- Card attributes ít thay đổi (credit_limit, expiry date là immutable sau khi issue)
- Card fraud flag (card_on_dark_web) cần update ngay, không cần history
- Không cần trace "card có credit_limit bao nhiêu lúc giao dịch X"
"""

from pathlib import Path

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import structlog

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent


def build_dim_cards(spark: SparkSession) -> dict:
    """
    SCD Type 1: merge staging → dim_cards.
    - New cards: INSERT
    - Existing cards: UPDATE (overwrite với latest values)
    """
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path = cfg["tables"]["cards"]["staging"]
    dim_path     = cfg["lake"]["warehouse"] + "/dim_cards"

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # ── Đọc staging — lấy latest record per card_id ──────────────────
    # Có thể có duplicate nếu cards được update nhiều lần
    # Dùng window để lấy record mới nhất
    from pyspark.sql.window import Window
    w = Window.partitionBy("card_id").orderBy(F.col("_ingested_at").desc())

    staging_df = spark.read.parquet(staging_path) \
        .withColumn("_rn", F.row_number().over(w)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn") \
        .select(
            "card_id", "user_id", "card_brand", "card_type",
            "card_number_masked", "expires_month", "expires_year",
            "has_chip", "num_cards_issued", "credit_limit",
            "acct_open_date", "year_pin_last_changed",
            "card_on_dark_web", "_batch_id"
        )

    # ── SCD Type 1: overwrite hoàn toàn ──────────────────────────────
    # Không cần merge phức tạp — chỉ cần latest snapshot
    # Write mode overwrite: thay thế toàn bộ dim_cards bằng staging mới nhất
    # count() TRƯỚC write() để tránh re-execute lineage lần 2
    count = staging_df.count()

    staging_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(dim_path)

    logger.info("build_dim_cards.done", total=count)

    return {"total": count}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("build_dim_cards")
    try:
        result = build_dim_cards(spark)
        print(f"dim_cards built: {result}")
    finally:
        stop_spark_session(spark)
