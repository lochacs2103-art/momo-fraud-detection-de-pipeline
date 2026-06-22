FROM apache/airflow:2.8.1-python3.10

USER airflow

# BuildKit cache: rebuild lần sau ~vài giây thay vì 20+ phút
# DOCKER_BUILDKIT=1 docker compose build airflow-webserver
COPY docker/airflow/requirements.txt /requirements.txt
RUN --mount=type=cache,target=/tmp/pip-cache,uid=50000,gid=0 \
    pip install --cache-dir=/tmp/pip-cache -r /requirements.txt
