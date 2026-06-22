FROM apache/airflow:2.8.1-python3.10

USER airflow

RUN pip install --no-cache-dir \
    "dbt-trino>=1.8.0,<1.9" \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyyaml==6.0.1 \
    structlog==23.2.0
