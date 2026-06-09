"""
build_dim_users.py — SCD Type 2 cho dimension users.

SCD Type 2 (Slowly Changing Dimension):
- Mỗi khi user thay đổi thông tin (address, income, credit_score)
  → tạo row mới với valid_from = now()
  → row cũ: valid_to = now(), is_current = False
- Query luôn JOIN với is_current = True để lấy thông tin mới nhất
- Query lịch sử: JOIN với valid_from <= target_date < valid_to

Tại sao cần SCD Type 2?
- Fraud analysis cần biết user có credit_score bao nhiêu VÀO THỜI ĐIỂM giao dịch
- Nếu chỉ overwrite (Type 1) → mất thông tin lịch sử → không trace được
"""

from pathlib import Path
from datetime import datetime

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import structlog

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Columns dùng để detect thay đổi (nếu khác → tạo row mới)
CHANGE_DETECT_COLS = [
    "address", "latitude", "longitude",
    "yearly_income", "total_debt", "credit_score",
    "per_capita_income", "current_age", "num_credit_cards"
]


def build_dim_users(spark: SparkSession) -> dict:
    """
    Full SCD Type 2 implementation:
    1. Đọc staging users (latest snapshot)
    2. So sánh với warehouse.dim_users hiện tại
    3. Với records thay đổi: close row cũ (valid_to=now, is_current=False)
                              insert row mới (valid_from=now, is_current=True)
    4. Records không đổi: giữ nguyên
    5. Records mới: insert với valid_from=now
    """
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path = cfg["tables"]["users"]["staging"]
    dim_path     = cfg["lake"]["warehouse"] + "/dim_users"
    now          = datetime.utcnow()
    FAR_FUTURE   = "9999-12-31 00:00:00"

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # ── 1. Đọc staging (latest state của mỗi user) ─────────────────────
    staging_df = spark.read.parquet(staging_path) \
        .dropDuplicates(["user_id"]) \
        .select(
            "user_id", "current_age", "retirement_age", "gender",
            "address", "latitude", "longitude",
            "per_capita_income", "yearly_income", "total_debt",
            "credit_score", "num_credit_cards", "_batch_id"
        )

    # ── 2. Đọc dim hiện tại ───────────────────────────────────────────
    try:
        dim_df = spark.read.parquet(dim_path)
        dim_exists = True
    except Exception:
        dim_exists = False
        dim_df = None

    if not dim_exists:
        # First load — tất cả đều là mới
        logger.info("build_dim_users.first_load")
        result_df = staging_df \
            .withColumn("valid_from",  F.lit(now.isoformat()).cast("timestamp")) \
            .withColumn("valid_to",    F.lit(FAR_FUTURE).cast("timestamp")) \
            .withColumn("is_current",  F.lit(True))

        result_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(dim_path)

        count = result_df.count()
        logger.info("build_dim_users.done", inserted=count, updated=0)
        return {"inserted": count, "updated": 0, "total": count}

    # ── 3. Detect changes ─────────────────────────────────────────────
    current_dim = dim_df.filter(F.col("is_current") == True) \
        .select(["user_id"] + CHANGE_DETECT_COLS)

    # Join staging với current dim để tìm:
    # a) New users (không có trong dim)
    # b) Changed users (có trong dim nhưng giá trị khác)
    # c) Unchanged users
    joined = staging_df.alias("s").join(
        current_dim.alias("d"),
        on="user_id",
        how="left"
    )

    # Build change condition: bất kỳ column nào khác nhau
    change_condition = None
    for col in CHANGE_DETECT_COLS:
        cond = F.col(f"s.{col}").isNull() != F.col(f"d.{col}").isNull()
        cond = cond | (
            F.col(f"s.{col}").isNotNull() &
            F.col(f"d.{col}").isNotNull() &
            (F.col(f"s.{col}") != F.col(f"d.{col}"))
        )
        change_condition = cond if change_condition is None else (change_condition | cond)

    is_new     = F.col("d.user_id").isNull()
    is_changed = (~is_new) & change_condition

    # ── 4. New rows để INSERT ─────────────────────────────────────────
    new_rows = joined.filter(is_new | is_changed) \
        .select([F.col(f"s.{c}").alias(c) for c in staging_df.columns]) \
        .withColumn("valid_from",  F.lit(now.isoformat()).cast("timestamp")) \
        .withColumn("valid_to",    F.lit(FAR_FUTURE).cast("timestamp")) \
        .withColumn("is_current",  F.lit(True))

    # ── 5. Close old rows (is_current=False, valid_to=now) ────────────
    changed_user_ids = joined.filter(is_changed).select("user_id")

    # Rows cần close
    rows_to_close = dim_df.join(
        F.broadcast(changed_user_ids),
        on="user_id",
        how="inner"
    ).filter(F.col("is_current") == True) \
     .withColumn("valid_to",   F.lit(now.isoformat()).cast("timestamp")) \
     .withColumn("is_current", F.lit(False))

    # Rows không thay đổi — giữ nguyên
    unchanged_rows = dim_df.join(
        F.broadcast(changed_user_ids),
        on="user_id",
        how="left_anti"  # loại trừ rows thuộc changed users
    )

    # ── 6. Union tất cả lại ──────────────────────────────────────────
    result_df = unchanged_rows \
        .unionByName(rows_to_close) \
        .unionByName(new_rows)

    result_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(dim_path)

    inserted_count = new_rows.count()
    updated_count  = rows_to_close.count()
    total_count    = result_df.count()

    logger.info("build_dim_users.done",
                inserted=inserted_count,
                updated=updated_count,
                total=total_count)

    return {
        "inserted": inserted_count,
        "updated":  updated_count,
        "total":    total_count,
    }


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("build_dim_users")
    try:
        result = build_dim_users(spark)
        print(f"dim_users built: {result}")
    finally:
        stop_spark_session(spark)
