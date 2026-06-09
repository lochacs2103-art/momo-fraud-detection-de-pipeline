"""
fraud_pipeline_dag.py — Main daily pipeline DAG.

Schedule: @daily lúc 02:00 AM
Catchup: True → Airflow tự backfill từng ngày nếu bị miss

Flow:
  [ingest group] → validate_raw → spark_staging → hive_repair
                → dbt_warehouse → dbt_test → notify

Mỗi run chỉ process đúng 1 ngày (execution_date).
→ Không bao giờ Spark job phải xử lý cả năm cùng lúc.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dags.utils.spark_utils import make_spark_submit, JDBC_JAR, SPARK_CONF

default_args = {
    "owner":            "de_team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="fraud_data_pipeline",
    description="Daily fraud detection data pipeline: ingest → staging → warehouse",
    schedule_interval="0 2 * * *",   # 02:00 AM every day
    start_date=datetime(2023, 1, 1),
    catchup=True,                    # backfill từng ngày
    max_active_runs=4,               # tối đa 4 ngày chạy song song
    default_args=default_args,
    tags=["fraud", "daily", "core"],
) as dag:

    # ── Ingestion group ────────────────────────────────────────────────────
    with TaskGroup("ingest", tooltip="JDBC ingest từ source DB → HDFS raw") as ingest_group:

        ingest_transactions = make_spark_submit(
            task_id="ingest_transactions",
            application="/opt/airflow/dags/../ingestion/jdbc_ingester.py",
            extra_conf={"spark.app.name": "ingest_transactions_{{ ds }}"},
        )

        ingest_users = make_spark_submit(
            task_id="ingest_users",
            application="/opt/airflow/dags/../ingestion/jdbc_ingester.py",
        )

        ingest_cards = make_spark_submit(
            task_id="ingest_cards",
            application="/opt/airflow/dags/../ingestion/jdbc_ingester.py",
        )

        ingest_mcc = make_spark_submit(
            task_id="ingest_mcc",
            application="/opt/airflow/dags/../ingestion/jdbc_ingester.py",
        )

        ingest_fraud_labels = make_spark_submit(
            task_id="ingest_fraud_labels",
            application="/opt/airflow/dags/../ingestion/jdbc_ingester.py",
        )

        # ingest_mcc và ingest_fraud_labels chạy song song (không phụ thuộc nhau)
        # ingest_transactions chạy song song với users/cards

    # ── Validate raw data ─────────────────────────────────────────────────
    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=(
            "python /opt/airflow/dags/../quality/run_checks.py "
            "--layer raw --date {{ ds }}"
        ),
    )

    # ── Staging transformation ─────────────────────────────────────────────
    with TaskGroup("staging", tooltip="Clean + Enrich → HDFS staging") as staging_group:

        clean_txn = make_spark_submit(
            task_id="clean_transactions",
            application="/opt/airflow/dags/../transformation/staging/clean_transactions.py",
            extra_conf={"spark.app.name": "clean_transactions_{{ ds }}"},
        )

        enrich_txn = make_spark_submit(
            task_id="enrich_transactions",
            application="/opt/airflow/dags/../transformation/staging/enrich_transactions.py",
        )

        clean_txn >> enrich_txn

    # ── MSCK REPAIR — sync partitions vào Hive Metastore ─────────────────
    # Quan trọng: sau khi Spark write, Hive Metastore chưa biết partitions mới
    # Nếu không repair → Trino query sẽ không thấy data mới
    # MSCK REPAIR nhanh hơn nhiều so với Spark list HDFS folders
    hive_repair = BashOperator(
        task_id="hive_msck_repair",
        bash_command="""
            beeline -u jdbc:hive2://hive-server:10000 -e "
                MSCK REPAIR TABLE raw.transactions;
                MSCK REPAIR TABLE staging.transactions;
                MSCK REPAIR TABLE staging.users;
                MSCK REPAIR TABLE staging.cards;
            "
        """,
    )

    # ── Build fraud features (Spark) ──────────────────────────────────────
    build_features = make_spark_submit(
        task_id="build_fraud_features",
        application="/opt/airflow/dags/../transformation/warehouse/build_fraud_features.py",
    )

    # ── dbt warehouse models ──────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run_warehouse",
        bash_command=(
            "cd /opt/airflow/dags/../dbt && "
            "dbt run --profiles-dir . --target prod "
            "--vars '{\"execution_date\": \"{{ ds }}\"}'"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dags/../dbt && "
            "dbt test --profiles-dir . --target prod"
        ),
    )

    # ── Notify ────────────────────────────────────────────────────────────
    def _check_quarantine(**context):
        """Branch: nếu có quarantine records → flag team, ngược lại → success."""
        # TODO: query quarantine table count
        quarantine_count = 0  # placeholder
        if quarantine_count > 0:
            return "flag_quarantine"
        return "notify_success"

    branch = BranchPythonOperator(
        task_id="check_quarantine",
        python_callable=_check_quarantine,
    )

    flag_quarantine = BashOperator(
        task_id="flag_quarantine",
        bash_command="echo 'QUARANTINE RECORDS FOUND on {{ ds }} — review needed'",
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Pipeline SUCCESS for {{ ds }}'",
    )

    # ── Task dependencies ─────────────────────────────────────────────────
    ingest_group >> validate_raw >> staging_group >> hive_repair
    hive_repair >> build_features >> dbt_run >> dbt_test >> branch
    branch >> [notify_success, flag_quarantine]
