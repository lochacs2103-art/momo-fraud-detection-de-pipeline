"""
compaction_pipeline_dag.py — Daily compaction + monthly tiering.

Schedule: 03:00 AM (sau fraud_pipeline_dag xong)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dags.utils.spark_utils import make_spark_submit

default_args = {
    "owner":        "de_team",
    "retries":      1,
    "retry_delay":  timedelta(minutes=10),
}

with DAG(
    dag_id="compaction_pipeline",
    description="Daily compaction (small file merge) + monthly tiering",
    schedule_interval="0 3 * * *",   # 03:00 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,                   # compaction không cần backfill
    default_args=default_args,
    tags=["compaction", "maintenance"],
) as dag:

    # ── Daily compaction ────────────────────────────────────────────────
    with TaskGroup("daily_compact") as daily_group:

        compact_transactions = make_spark_submit(
            task_id="compact_transactions_yesterday",
            application="/opt/airflow/dags/../transformation/compaction/compactor.py",
            extra_conf={"spark.app.name": "compact_transactions_{{ ds }}"},
        )

        compact_users = make_spark_submit(
            task_id="compact_users_yesterday",
            application="/opt/airflow/dags/../transformation/compaction/compactor.py",
        )

        compact_cards = make_spark_submit(
            task_id="compact_cards_yesterday",
            application="/opt/airflow/dags/../transformation/compaction/compactor.py",
        )
        # Chạy song song — không phụ thuộc nhau

    # ── Monthly tiering (chỉ chạy ngày 1 hàng tháng) ──────────────────
    def _should_run_monthly(**context):
        """Chỉ trigger monthly compact vào ngày 1 của tháng."""
        return context["execution_date"].day == 1

    monthly_check = PythonOperator(
        task_id="check_monthly_trigger",
        python_callable=_should_run_monthly,
    )

    with TaskGroup("monthly_tier") as monthly_group:

        tier_transactions = make_spark_submit(
            task_id="tier_transactions_to_warm",
            application="/opt/airflow/dags/../transformation/compaction/compactor.py",
        )

        tier_users = make_spark_submit(
            task_id="tier_users_to_warm",
            application="/opt/airflow/dags/../transformation/compaction/compactor.py",
        )

    # ── Vacuum quarantine (xóa records resolved > 30 ngày) ─────────────
    vacuum_quarantine = BashOperator(
        task_id="vacuum_quarantine",
        bash_command=(
            "python /opt/airflow/dags/../quality/checks/vacuum_quarantine.py "
            "--retention-days 30"
        ),
    )

    # ── Dependencies ────────────────────────────────────────────────────
    daily_group >> monthly_check >> monthly_group
    daily_group >> vacuum_quarantine
