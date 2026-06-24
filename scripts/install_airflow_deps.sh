#!/usr/bin/env bash
# Cài dbt + Spark provider vào Airflow container (1 lần, ~2–5 phút).
set -euo pipefail

REQ="/opt/project/docker/airflow/requirements.txt"

if ! docker ps --format '{{.Names}}' | grep -q '^airflow-webserver$'; then
  echo "airflow-webserver chưa chạy. Chạy: cd docker && docker compose up -d"
  exit 1
fi

if ! docker exec airflow-webserver test -f "$REQ"; then
  echo "ERROR: Không thấy $REQ trong container."
  echo "  → Chạy lại: cd docker && docker compose up -d"
  echo "  → Cần volume mount ../:/opt/project"
  exit 1
fi

echo "Installing Airflow Python deps (dbt-trino, spark provider)..."
docker exec -u airflow airflow-webserver python -m pip install --user \
  -r "$REQ"

echo "Verifying dbt..."
docker exec -u airflow airflow-webserver python -m dbt --version

echo "Done. dbt persisted in volume de_airflow_pip (survives container restart)."
