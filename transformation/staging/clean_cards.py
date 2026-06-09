"""
clean_cards.py — Raw → Staging transformation cho cards.

Cleaning steps:
1. PCI: card_number → masked, cvv → DROP
2. expires parse: "MM/YYYY" → expires_month (INT), expires_year (INT)
3. credit_limit: "$24295" → 24295.0
4. has_chip: "YES"/"NO" → BOOLEAN
5. card_on_dark_web: "Yes"/"No" → BOOLEAN
6. account_age_months: số tháng từ acct_open_date đến ngày ingest
"""

from pathlib import Path
from datetime import date

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


def _pci_mask_cards(df: DataFrame) -> DataFrame:
    """Mask card_number, drop cvv."""
    import re as _re
    mask_udf = F.udf(
        lambda cn: f"XXXX-XXXX-XXXX-{_re.sub(r'[^0-9]', '', str(cn or ''))[-4:].zfill(4)}"
        if cn else None
    )
    if "card_number" in df.columns:
        df = df.withColumn("card_number_masked", mask_udf(F.col("card_number"))) \
               .drop("card_number")
    if "cvv" in df.columns:
        df = df.drop("cvv")
    return df


def _parse_expires(df: DataFrame) -> DataFrame:
    """
    Q10: Parse "MM/YYYY" → expires_month INT, expires_year INT.
    "12/2022" → month=12, year=2022
    """
    return df \
        .withColumn("expires_month",
                    F.split(F.col("expires"), "/").getItem(0).cast("int")) \
        .withColumn("expires_year",
                    F.split(F.col("expires"), "/").getItem(1).cast("int")) \
        .drop("expires")


def _clean_credit_limit(df: DataFrame) -> DataFrame:
    """Strip '$', cast to DOUBLE."""
    return df.withColumn(
        "credit_limit",
        F.regexp_replace(F.col("credit_limit"), r"[$,]", "").cast("double")
    )


def _cast_booleans(df: DataFrame) -> DataFrame:
    """
    Q11: card_on_dark_web "Yes"/"No" → BOOLEAN.
    has_chip "YES"/"NO" → BOOLEAN.
    """
    return df \
        .withColumn("has_chip",
                    F.upper(F.col("has_chip")) == "YES") \
        .withColumn("card_on_dark_web",
                    F.upper(F.col("card_on_dark_web")) == "YES")


def _add_account_age(df: DataFrame, ingest_date: date) -> DataFrame:
    """
    Q12: account_age_months = số tháng từ acct_open_date đến ingest_date.
    "09/2002" → tính tháng chênh lệch.
    Lý do: card mới mở (< 3 tháng) là fraud risk signal.
    """
    @F.udf("int")
    def _months_diff(open_date_str, ref_year, ref_month):
        if open_date_str is None:
            return None
        try:
            parts = open_date_str.split("/")
            open_month = int(parts[0])
            open_year  = int(parts[1])
            return (ref_year - open_year) * 12 + (ref_month - open_month)
        except Exception:
            return None

    return df.withColumn(
        "account_age_months",
        _months_diff(
            F.col("acct_open_date"),
            F.lit(ingest_date.year),
            F.lit(ingest_date.month)
        )
    )


def clean_cards(spark: SparkSession, ingest_date: date = None) -> dict:
    """Clean toàn bộ cards table."""
    cfg = _load_config()
    raw_path     = cfg["tables"]["cards"]["raw"]
    staging_path = cfg["tables"]["cards"]["staging"]

    if ingest_date is None:
        ingest_date = date.today()

    logger.info("clean_cards.start")

    df = spark.read.parquet(raw_path)
    raw_count = df.count()

    # Rename
    df = df.withColumnRenamed("id",        "card_id") \
           .withColumnRenamed("client_id", "user_id")

    # Apply cleaning
    df = _pci_mask_cards(df)
    df = _parse_expires(df)
    df = _clean_credit_limit(df)
    df = _cast_booleans(df)
    df = _add_account_age(df, ingest_date)
    df = df.withColumn("num_cards_issued",     F.col("num_cards_issued").cast("int")) \
           .withColumn("year_pin_last_changed", F.col("year_pin_last_changed").cast("int"))

    # is_valid
    df = df.withColumn("is_valid", F.col("card_id").isNotNull())

    # Dedup
    w = Window.partitionBy("card_id").orderBy(F.col("_loaded_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    # Write
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.repartition(F.col("card_brand_part"), F.col("expires_year_part")) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .partitionBy("card_brand_part", "expires_year_part") \
      .parquet(staging_path)

    count = df.count()
    logger.info("clean_cards.done", raw=raw_count, staged=count)
    return {"raw_count": raw_count, "staged_count": count}


if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session("clean_cards")
    try:
        result = clean_cards(spark)
        print(result)
    finally:
        stop_spark_session(spark)
