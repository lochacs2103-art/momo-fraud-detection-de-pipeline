#!/usr/bin/env bash
# Full end-to-end pipeline cho static dataset (backfill mode).
# Chạy từ project root: bash scripts/run_e2e.sh
#
# Prerequisites:
#   make download-jars && make copy-data && make up && make hdfs-init && make hive-init

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> [1/7] Ingest: PostgreSQL → HDFS raw"
make ingest

echo "==> [2/7] Transform: raw → staging (full backfill)"
make transform-full

echo "==> [3/7] Warehouse: Spark features + dims"
make transform-warehouse

echo "==> [4/7] Hive: sync partitions"
make hive-repair

echo "==> [5/7] dbt: staging → warehouse → marts"
make dbt-run

echo "==> [6/7] dbt: data tests"
make dbt-test

echo "==> [7/7] Smoke test via Trino"
docker exec trino trino --execute "
SELECT 'fact_transactions' AS tbl, COUNT(*) AS rows FROM hive.warehouse.fact_transactions
UNION ALL
SELECT 'fraud_features', COUNT(*) FROM hive.warehouse.fraud_features
UNION ALL
SELECT 'user_daily_stats', COUNT(*) FROM hive.warehouse.user_daily_stats
UNION ALL
SELECT 'merchant_risk_score', COUNT(*) FROM hive.warehouse.merchant_risk_score
"

echo ""
echo "=== E2E pipeline complete ==="
echo "  Trino UI:    http://localhost:8082"
echo "  Superset UI: http://localhost:8088  (admin/admin)"
echo "  Connect Superset → trino://trino@trino:8080/hive"
