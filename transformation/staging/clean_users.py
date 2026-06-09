"""
clean_users.py — Raw → Staging transformation cho users.

Cleaning steps:
1. Cast income fields ($29278 → 29278.0)
2. credit_score_band (POOR/FAIR/GOOD/VERY_GOOD/EXCEPTIONAL/INVALID)
3. is_invalid_credit_score flag
4. age_group (TEEN/YOUNG_ADULT/ADULT/MIDDLE_AGED/SENIOR)
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


def _load_config():
    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        return yaml.safe_load(f)


def _cast_income_fields(df: DataFrame) -> DataFrame:
    """Strip '$' và cast sang DOUBLE."""
    for col in ["per_capita_income", "yearly_income", "total_debt"]:
        df = df.withColumn(
            col,
            F.regexp_replace(F.col(col), r"[$,]", "").cast("double")
        )
    return df


def _add_credit_score_band(df: DataFrame) -> DataFrame:
    """
    Q8: credit_score_band theo chuẩn FICO.
    INVALID nếu ngoài 300–850.
    """
    cs = F.col("credit_score").cast("int")
    band = (
        F.when(cs.isNull(),              F.lit(None).cast("string"))
         .when((cs < 300) | (cs > 850),  F.lit("INVALID"))
         .when(cs < 580,                 F.lit("POOR"))
         .when(cs < 670,                 F.lit("FAIR"))
         .when(cs < 740,                 F.lit("GOOD"))
         .when(cs < 800,                 F.lit("VERY_GOOD"))
         .otherwise(                     F.lit("EXCEPTIONAL"))
    )
    return df \
        .withColumn("credit_score", cs) \
        .withColumn("credit_score_band", band) \
        .withColumn("is_invalid_credit_score",
                    F.when(cs.isNotNull(), (cs < 300) | (cs > 850))
                     .otherwise(F.lit(False)))


def _add_age_group(df: DataFrame) -> DataFrame:
    """
    Q9: age_group — tên thân thiện, nhìn vào biết ngay.
    TEEN / YOUNG_ADULT / ADULT / MIDDLE_AGED / SENIOR
    """
    age = F.col("current_age").cast("int")
    group = (
        F.when(age.isNull(),    F.lit(None).cast("string"))
         .when(age < 18,        F.lit("TEEN"))
         .when(age <= 25,       F.lit("YOUNG_ADULT"))
         .when(age <= 40,       F.lit("ADULT"))
         .when(age <= 60,       F.lit("MIDDLE_AGED"))
         .otherwise(            F.lit("SENIOR"))
    )
    return df \
        .withColumn("current_age", age) \
        .withColumn("retirement_age", F.col("retirement_age").cast("int")) \
        .withColumn("age_group", group)


def clean_users(spark: SparkSession) -> dict:
    """Clean toàn bộ users table (không partition theo ngày như transactions)."""
    cfg = _load_config()
    raw_path     = cfg["tables"]["users"]["raw"]
    staging_path = cfg["tables"]["users"]["staging"]

    logger.info("clean_users.start")

    df = spark.read.parquet(raw_path)
    raw_count = df.count()

    # Rename id → user_id
    df = df.withColumnRenamed("id", "user_id")

    # Apply cleaning
    df = _cast_income_fields(df)
    df = _add_credit_score_band(df)
    df = _add_age_group(df)

    # is_valid: user_id not null
    df = df.withColumn("is_valid", F.col("user_id").isNotNull())

    # Dedup: giữ _loaded_at mới nhất
    w = Window.partitionBy("user_id").orderBy(F.col("_loaded_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    # Write
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.repartition(F.col("created_year"), F.col("created_month")) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("created_year", "created_month") \
      .parquet(staging_path)

    count = df.count()
    logger.info("clean_users.done", raw=raw_count, staged=count)
    return {"raw_count": raw_count, "staged_count": count}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("clean_users")
    try:
        result = clean_users(spark)
        print(result)
    finally:
        stop_spark_session(spark)
