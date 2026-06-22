#!/usr/bin/env bash
# Cài dbt + Spark provider vào Airflow container đang chạy (1 lần, ~2–5 phút).
# Không cần docker compose --build.
set -euo pipefail

if ! docker ps --format '{{.Names}}' | grep -q '^airflow-webserver$'; then
  echo "airflow-webserver chưa chạy. Chạy: cd docker && docker compose up -d"
  exit 1
fi

echo "Installing Airflow Python deps (dbt-trino, spark provider)..."
docker exec airflow-webserver pip install -q \
  -r /opt/project/docker/airflow/requirements.txt

echo "Verifying dbt..."
docker exec airflow-webserver /home/airflow/.local/bin/dbt --version

echo "Done."
