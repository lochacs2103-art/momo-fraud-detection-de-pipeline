# Makefile — shortcut commands cho toàn bộ project
# Chạy: make <target>
# Ví dụ: make up, make ingest, make transform

.PHONY: help up down ps logs ingest transform test clean

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
	@echo "  make ingest    — Chạy ingestion (source DB → HDFS raw)"
	@echo "  make transform — Chạy transformation (staging layer)"
	@echo "  make compact   — Chạy compaction job"
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
	@echo "Running transformation jobs..."
	# TODO: implement

compact:
	@echo "Running compaction jobs..."
	# TODO: implement

# ---- Development ----

test:
	SPARK_LOCAL_MODE=true pytest tests/ -v

lint:
	flake8 ingestion/ transformation/ airflow/ --max-line-length=120

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
