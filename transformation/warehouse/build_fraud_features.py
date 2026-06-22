"""
build_fraud_features.py — Tính velocity và anomaly features cho fraud detection.

Features:
- Velocity: txn_count_last_1h, txn_count_last_24h, txn_count_last_7d
- Amount anomaly: amount_vs_user_avg_ratio
- Time: is_night_txn, is_weekend
- Risk: card_on_dark_web

Dùng Spark Window Functions — không cần groupBy/join phức tạp.
"""

from pathlib import Path
from datetime import date, timedelta

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import structlog

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent


def build_fraud_features(spark: SparkSession, execution_date: date) -> dict:
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    year, month, day = execution_date.year, execution_date.month, execution_date.day

    # Đọc staging của ngày này + 7 ngày trước (cần để tính velocity 7d)
    # Partition pruning: chỉ đọc 8 ngày, không scan toàn bộ
    lookback_date = execution_date - timedelta(days=7)
    staging_path = cfg["tables"]["transactions"]["staging"]

    df = spark.read.parquet(staging_path).filter(
        (F.col("year") * 10000 + F.col("month") * 100 + F.col("day") >=
         lookback_date.year * 10000 + lookback_date.month * 100 + lookback_date.day) &
        (F.col("year") * 10000 + F.col("month") * 100 + F.col("day") <=
         year * 10000 + month * 100 + day)
    ).filter(F.col("is_valid") == True)

    # Convert transaction_date → unix timestamp để dùng range window
    df = df.withColumn("ts", F.unix_timestamp("transaction_date"))

    # ── Window definitions ────────────────────────────────────────────────
    # Range window: "tất cả rows của cùng user_id trong N giây trước"
    # Cần ORDER BY ts (numeric) để dùng rangeBetween

    w_user = Window.partitionBy("user_id").orderBy("ts")

    w_1h = w_user.rangeBetween(
        -3600,          # 1 giờ trước (giây)
        Window.currentRow
    )
    w_24h = w_user.rangeBetween(-86400, Window.currentRow)   # 24h
    w_7d  = w_user.rangeBetween(-604800, Window.currentRow)  # 7 ngày

    # User lifetime window (cho avg amount)
    w_user_all = Window.partitionBy("user_id").orderBy("ts") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # ── Tính features ─────────────────────────────────────────────────────
    df = df \
        .withColumn("txn_count_last_1h",  F.count("transaction_id").over(w_1h)) \
        .withColumn("txn_count_last_24h", F.count("transaction_id").over(w_24h)) \
        .withColumn("txn_count_last_7d",  F.count("transaction_id").over(w_7d)) \
        .withColumn("amount_sum_last_1h", F.sum("amount").over(w_1h)) \
        .withColumn("amount_sum_last_24h",F.sum("amount").over(w_24h)) \
        .withColumn("user_avg_amount",    F.avg("amount").over(w_user_all)) \
        .withColumn("amount_vs_user_avg_ratio",
            F.when(F.col("user_avg_amount") > 0,
                   F.col("amount") / F.col("user_avg_amount")
            ).otherwise(F.lit(None))
        ) \
        .withColumn("is_night_txn",
            F.hour("transaction_date").between(0, 5)
        ) \
        .withColumn("is_weekend",
            F.dayofweek("transaction_date").isin([1, 7])  # 1=Sun, 7=Sat
        ) \
        .withColumn("is_foreign_merchant",
            ~F.col("merchant_state").isin([
                "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
                "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
                "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
                "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
                "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
            ])
        )

    # ── Chỉ giữ features của ngày target (không lưu 7 ngày lookback) ─────
    df_features = df.filter(
        (F.col("year")  == year) &
        (F.col("month") == month) &
        (F.col("day")   == day)
    ).select(
        "transaction_id", "user_id",
        "txn_count_last_1h", "txn_count_last_24h", "txn_count_last_7d",
        F.col("amount_sum_last_1h").cast("double").alias("amount_sum_last_1h"),
        F.col("amount_sum_last_24h").cast("double").alias("amount_sum_last_24h"),
        F.col("amount_vs_user_avg_ratio").cast("double").alias("amount_vs_user_avg_ratio"),
        "is_night_txn", "is_weekend", "is_foreign_merchant",
        "card_on_dark_web",
        "is_fraud",
        F.lit(None).cast("double").alias("risk_score"),  # ML model output (future)
        "_batch_id",
        F.lit(year).alias("year"),
        F.lit(month).alias("month"),
        F.lit(day).alias("day"),
    )

    # ── Write ─────────────────────────────────────────────────────────────
    features_path = cfg["lake"]["warehouse"] + "/feat_fraud_features"
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_features \
        .repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(features_path)

    row_count = df_features.count()
    logger.info("build_fraud_features.done",
                date=execution_date.isoformat(),
                row_count=row_count)

    return {"date": execution_date.isoformat(), "row_count": row_count}


def build_fraud_features_full(spark: SparkSession) -> dict:
    """
    Backfill mode: tính features cho TOÀN BỘ data trong 1 Spark job.
    Không loop theo ngày — 1 job duy nhất, Spark tự parallel hóa.
    """
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    staging_path  = cfg["tables"]["transactions"]["staging"]
    features_path = cfg["lake"]["warehouse"] + "/feat_fraud_features"

    df = spark.read.parquet(staging_path).filter(F.col("is_valid") == True)
    df = df.withColumn("ts", F.unix_timestamp("transaction_date"))

    w_user = Window.partitionBy("user_id").orderBy("ts")
    w_1h   = w_user.rangeBetween(-3600,   Window.currentRow)
    w_24h  = w_user.rangeBetween(-86400,  Window.currentRow)
    w_7d   = w_user.rangeBetween(-604800, Window.currentRow)
    w_user_all = Window.partitionBy("user_id").orderBy("ts") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df = df \
        .withColumn("txn_count_last_1h",   F.count("transaction_id").over(w_1h)) \
        .withColumn("txn_count_last_24h",  F.count("transaction_id").over(w_24h)) \
        .withColumn("txn_count_last_7d",   F.count("transaction_id").over(w_7d)) \
        .withColumn("amount_sum_last_1h",  F.sum("amount").over(w_1h)) \
        .withColumn("amount_sum_last_24h", F.sum("amount").over(w_24h)) \
        .withColumn("user_avg_amount",     F.avg("amount").over(w_user_all)) \
        .withColumn("amount_vs_user_avg_ratio",
            F.when(F.col("user_avg_amount") > 0,
                   F.col("amount") / F.col("user_avg_amount")
            ).otherwise(F.lit(None))
        ) \
        .withColumn("is_night_txn",  F.hour("transaction_date").between(0, 5)) \
        .withColumn("is_weekend",    F.dayofweek("transaction_date").isin([1, 7])) \
        .withColumn("is_foreign_merchant",
            ~F.col("merchant_state").isin([
                "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
                "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
                "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
                "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
                "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
            ])
        )

    # is_fraud: fraud_labels không được join ở staging (OOM với 8.9M rows)
    # → NULL ở đây, sẽ được join ở dbt/warehouse layer sau
    # card_on_dark_web: chỉ có sau enrich, có thể NULL nếu chưa enrich
    card_on_dark_web_col = F.col("card_on_dark_web") \
        if "card_on_dark_web" in df.columns \
        else F.lit(None).cast("boolean")

    df_features = df.select(
        "transaction_id", "user_id",
        "txn_count_last_1h", "txn_count_last_24h", "txn_count_last_7d",
        F.col("amount_sum_last_1h").cast("double").alias("amount_sum_last_1h"),
        F.col("amount_sum_last_24h").cast("double").alias("amount_sum_last_24h"),
        F.col("amount_vs_user_avg_ratio").cast("double").alias("amount_vs_user_avg_ratio"),
        "is_night_txn", "is_weekend", "is_foreign_merchant",
        card_on_dark_web_col.alias("card_on_dark_web"),
        F.lit(None).cast("boolean").alias("is_fraud"),   # populated in warehouse/dbt
        F.lit(None).cast("double").alias("risk_score"),
        "_batch_id",
        "year", "month", "day",
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df_features \
        .repartition(F.col("year"), F.col("month"), F.col("day")) \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month", "day") \
        .parquet(features_path)

    row_count = df_features.count()
    logger.info("build_fraud_features_full.done", row_count=row_count)
    return {"row_count": row_count}


if __name__ == "__main__":
    import sys
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("build_fraud_features")
    try:
        if len(sys.argv) > 1:
            # Single date mode: spark-submit ... build_fraud_features.py 2019-06-15
            exec_date = date.fromisoformat(sys.argv[1])
            result = build_fraud_features(spark, exec_date)
        else:
            # Backfill mode: toàn bộ data trong 1 job
            result = build_fraud_features_full(spark)
        print(f"\n=== DONE: {result} ===")
    finally:
        stop_spark_session(spark)
