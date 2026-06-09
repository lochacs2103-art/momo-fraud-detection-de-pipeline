# Makefile — shortcut commands cho toàn bộ project
# Chạy: make <target>
# Ví dụ: make up, make ingest, make transform

.PHONY: help up down ps logs ingest transform transform-warehouse compact dbt-run dbt-test pipeline test clean

# Default target
help:
	@echo "MoMo Fraud Detection Pipeline"
	@echo ""
	@echo "Setup (chạy lần đầu theo thứ tự):"
	@echo "  make download-jars  — Download PostgreSQL JDBC driver"
	@echo "  make up             — Start toàn bộ Docker stack"
	@echo "  make hdfs-init      — Tạo HDFS dirs + set Storage Policies (SSD/HDD)"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up             — Start toàn bộ Docker stack"
	@echo "  make down           — Stop và remove containers"
	@echo "  make ps             — Xem trạng thái containers"
	@echo "  make logs           — Xem logs tất cả services"
	@echo "  make hdfs-ls        — List files trên HDFS"
	@echo "  make hdfs-policies  — Xem storage policies hiện tại"
	@echo ""
	@echo "Pipeline:"
	@echo "  make ingest             — Ingest: source DB → HDFS raw (JDBC parallel)"
	@echo "  make transform          — Transform: raw → staging (clean + enrich)"
	@echo "  make transform-warehouse — Build warehouse: dims (SCD1/2) + fraud features"
	@echo "  make compact            — Compaction: merge small files, sort by user_id"
	@echo "  make dbt-run            — Run dbt models (staging → warehouse → marts)"
	@echo "  make dbt-test           — Run dbt tests"
	@echo "  make pipeline           — Run full pipeline end-to-end"
	@echo ""
	@echo "Development:"
	@echo "  make test      — Chạy unit tests"
	@echo "  make lint      — Chạy linter"
	@echo "  make clean     — Xóa temp files"

# ---- Infrastructure ----

download-jars:
	@echo "Downloading JDBC drivers..."
	bash docker/spark/download_jars.sh

up:
	@echo "Starting DE stack..."
	docker compose -f docker/docker-compose.yml up -d
	@echo ""
	@echo "Services:"
	@echo "  Source DB:      postgresql://localhost:5432/momo_source  (momo/momo)"
	@echo "  HDFS Web UI:    http://localhost:9870"
	@echo "  Spark Web UI:   http://localhost:8081"
	@echo "  Trino UI:       http://localhost:8082"
	@echo "  Airflow UI:     http://localhost:8083  (admin/admin)"
	@echo "  Superset UI:    http://localhost:8088  (admin/admin)"

source-db-check:
	@echo "Checking source DB row counts..."
	docker exec source-db psql -U momo -d momo_source -c \
		"SELECT tablename, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"

down:
	docker compose -f docker/docker-compose.yml down

down-clean:
	@echo "WARNING: This will delete all volumes (HDFS data, Metastore, Airflow state)"
	docker compose -f docker/docker-compose.yml down -v

ps:
	docker compose -f docker/docker-compose.yml ps

logs:
	docker compose -f docker/docker-compose.yml logs -f

logs-%:
	docker compose -f docker/docker-compose.yml logs -f $*

# ---- HDFS Setup (chạy lần đầu sau khi up) ----

hdfs-init:
	@echo "Setting up HDFS directories and storage policies..."
	bash docker/hadoop/setup_storage_policies.sh

hdfs-policies:
	@echo "Current storage policies:"
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/raw
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/staging
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/warm
	docker exec namenode hdfs storagepolicies -getStoragePolicy -path /data/lake/cold

hdfs-ls:
	docker exec namenode hdfs dfs -ls -R /data/lake/

hive-init:
	@echo "Creating Hive databases and external tables..."
	docker exec hive-server beeline -u jdbc:hive2://localhost:10000 \
		-f /opt/hive/conf/../../../docker/hive/init_hive_schemas.sql
	@echo "Hive schemas created."

# ---- Pipeline ----

ingest:
	@echo "Running JDBC ingestion jobs (source DB → HDFS raw)..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--jars /opt/bitnami/spark/extra-jars/postgresql-42.7.1.jar \
		/opt/bitnami/spark/work-dir/ingestion/jdbc_ingester.py

transform:
	@echo "Running transformation jobs (raw → staging)..."
	@echo "Step 1: Clean transactions..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--jars /opt/bitnami/spark/extra-jars/postgresql-42.7.1.jar \
		/opt/bitnami/spark/work-dir/transformation/staging/clean_transactions.py
	@echo "Step 2: Clean users..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/work-dir/transformation/staging/clean_users.py
	@echo "Step 3: Clean cards..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/work-dir/transformation/staging/clean_cards.py
	@echo "Step 4: Enrich transactions (broadcast join mcc/cards/fraud)..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/work-dir/transformation/staging/enrich_transactions.py
	@echo "Transformation complete."

transform-warehouse:
	@echo "Running warehouse build jobs..."
	@echo "Step 1: Build dim_users (SCD Type 2)..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/work-dir/transformation/warehouse/build_dim_users.py
	@echo "Step 2: Build dim_cards (SCD Type 1)..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/work-dir/transformation/warehouse/build_dim_cards.py
	@echo "Step 3: Build fraud features (window functions)..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/work-dir/transformation/warehouse/build_fraud_features.py
	@echo "Warehouse build complete."

compact:
	@echo "Running compaction job (merge small files, sort by user_id)..."
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/bitnami/spark/work-dir/transformation/compaction/compactor.py
	@echo "Compaction complete."

dbt-run:
	@echo "Running dbt models (staging → warehouse → marts)..."
	docker exec trino dbt run \
		--profiles-dir /opt/dbt \
		--project-dir /opt/dbt \
		--target prod

dbt-test:
	@echo "Running dbt tests..."
	docker exec trino dbt test \
		--profiles-dir /opt/dbt \
		--project-dir /opt/dbt \
		--target prod

pipeline:
	@echo "Running full pipeline: ingest → transform → warehouse → dbt..."
	make ingest
	make transform
	make transform-warehouse
	make dbt-run
	make dbt-test
	@echo "Full pipeline complete."

# ---- Development ----

test:
	SPARK_LOCAL_MODE=true pytest tests/ -v

lint:
	flake8 ingestion/ transformation/ airflow/ --max-line-length=120

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
