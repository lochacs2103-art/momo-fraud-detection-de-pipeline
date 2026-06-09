"""Airflow utilities cho Spark job submission."""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
JDBC_JAR      = "/opt/bitnami/spark/extra-jars/postgresql-42.7.1.jar"

SPARK_CONF = {
    "spark.sql.adaptive.enabled":               "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled":      "true",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.executor.memory":                    "4g",
    "spark.executor.memoryOverhead":            "1g",
    "spark.memory.fraction":                    "0.6",
    "spark.memory.storageFraction":             "0.3",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}


def make_spark_submit(task_id: str, application: str, extra_conf: dict = None) -> SparkSubmitOperator:
    conf = {**SPARK_CONF, **(extra_conf or {})}
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=application,
        jars=JDBC_JAR,
        conf=conf,
        verbose=False,
    )
