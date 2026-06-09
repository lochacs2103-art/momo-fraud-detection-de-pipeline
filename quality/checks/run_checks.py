"""
run_checks.py — Data quality checks cho từng layer.
Được gọi bởi Airflow DAG sau mỗi ingestion.
"""

import argparse
import sys
from pathlib import Path
from datetime import date

import yaml

PROJECT_ROOT = Path(__file__).parent.parent.parent


def run_raw_checks(check_date: date) -> dict:
    """Basic checks trên raw layer — row count, null keys."""
    from ingestion.spark_session import get_spark_session, stop_spark_session
    from pyspark.sql import functions as F

    with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
        cfg = yaml.safe_load(f)

    spark = get_spark_session("data_quality_raw")
    results = {}

    try:
        year, month, day = check_date.year, check_date.month, check_date.day

        # Check transactions
        txn_path = (
            f"{cfg['tables']['transactions']['raw']}"
            f"/year={year}/month={month}/day={day}"
        )
        try:
            df = spark.read.parquet(txn_path)
            row_count = df.count()
            null_id_count = df.filter(F.col("id").isNull()).count()
            results["transactions"] = {
                "row_count": row_count,
                "null_id_count": null_id_count,
                "passed": row_count > 0 and null_id_count == 0,
            }
        except Exception as e:
            results["transactions"] = {"passed": False, "error": str(e)}

        # Check mcc_codes
        try:
            mcc_df = spark.read.parquet(cfg["tables"]["mcc_codes"]["raw"])
            results["mcc_codes"] = {
                "row_count": mcc_df.count(),
                "passed": mcc_df.count() > 0,
            }
        except Exception as e:
            results["mcc_codes"] = {"passed": False, "error": str(e)}

        # Overall
        all_passed = all(r.get("passed", False) for r in results.values())
        if not all_passed:
            failed = [k for k, v in results.items() if not v.get("passed")]
            print(f"QUALITY CHECK FAILED: {failed}", file=sys.stderr)
            sys.exit(1)

        print(f"Quality checks PASSED for {check_date}: {results}")
        return results

    finally:
        stop_spark_session(spark)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=["raw", "staging"], default="raw")
    parser.add_argument("--date", type=str, default=str(date.today()))
    args = parser.parse_args()

    check_date = date.fromisoformat(args.date)

    if args.layer == "raw":
        run_raw_checks(check_date)


if __name__ == "__main__":
    main()
