#!/usr/bin/env bash
# Cài dbt + Spark provider vào Airflow container (1 lần, ~1–3 phút).
set -euo pipefail

REQ="/opt/project/docker/airflow/requirements.txt"
PIP="python -m pip install --user --default-timeout=300"

if ! docker ps --format '{{.Names}}' | grep -q '^airflow-webserver$'; then
  echo "airflow-webserver chưa chạy. Chạy: cd docker && docker compose up -d"
  exit 1
fi

if ! docker exec airflow-webserver test -f "$REQ"; then
  echo "ERROR: Không thấy $REQ trong container."
  echo "  → Chạy lại: cd docker && docker compose up -d"
  exit 1
fi

echo "[1/2] Installing dbt-trino (dbt-core pinned)..."
docker exec -u airflow airflow-webserver bash -lc \
  "$PIP dbt-trino==1.8.5 dbt-core==1.8.9"

echo "[2/2] Installing Spark provider (--no-deps, skip pyspark 455MB)..."
docker exec -u airflow airflow-webserver bash -lc \
  "$PIP --no-deps apache-airflow-providers-apache-spark==4.7.1"

echo "Verifying dbt..."
docker exec -u airflow airflow-webserver bash -lc \
  'export PATH="/home/airflow/.local/bin:$PATH" && dbt --version'

echo "Done. Packages persisted in volume de_airflow_pip."
