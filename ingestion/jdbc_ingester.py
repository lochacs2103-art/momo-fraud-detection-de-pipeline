"""
JDBCIngester — đọc data từ PostgreSQL source DB qua JDBC, ghi vào HDFS raw layer.

Tại sao JDBC thay vì đọc CSV trực tiếp?
- Thực tế: trong doanh nghiệp, data nằm trong database (MySQL, PostgreSQL, Oracle...)
  không phải file. DE job phải kết nối DB và pull data ra.
- JDBC = Java Database Connectivity — giao thức chuẩn để kết nối DB từ JVM (Spark chạy trên JVM)
- Spark có built-in JDBC reader: spark.read.jdbc(...)

Vấn đề lớn nhất của JDBC ingest: PERFORMANCE
- Nếu đọc single-threaded: 1 connection, đọc từng row → cực chậm với bảng lớn
- Cần parallel read: chia bảng thành N partitions, mỗi partition = 1 connection chạy song song

Kỹ thuật parallel read với Spark JDBC:
  Spark sẽ tạo N queries: WHERE partition_col BETWEEN lower AND upper
  Mỗi query chạy trên 1 executor task song song → throughput tăng N lần
"""

from pathlib import Path
from typing import Optional
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import structlog

from ingestion.base_ingester import BaseIngester

logger = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent


class JDBCIngester(BaseIngester):
    """
    Base class cho tất cả JDBC-based ingesters.
    Subclass chỉ cần implement:
    - get_source_table(): tên table trong source DB
    - get_raw_schema(): StructType (optional, nếu muốn enforce schema)
    - get_hdfs_output_path(): HDFS output path
    - get_partition_cols(): list partition columns
    - add_partition_cols(): thêm partition columns vào DataFrame
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self._load_db_config()

    def _load_db_config(self):
        with open(PROJECT_ROOT / "config" / "source_db.yaml") as f:
            self.db_config = yaml.safe_load(f)

    def get_source_table(self) -> str:
        """Tên table trong source DB. Subclass override."""
        raise NotImplementedError

    def get_source_path(self) -> str:
        """Override từ BaseIngester — với JDBC, 'path' là table name."""
        return f"{self.db_config['jdbc_url']}/{self.get_source_table()}"

    def _get_jdbc_options(self) -> dict:
        """Base JDBC options dùng cho tất cả tables."""
        return {
            "url":      self.db_config["jdbc_url"],
            "driver":   self.db_config["driver"],
            "user":     self.db_config["user"],
            "password": self.db_config["password"],
            "fetchsize": str(self.db_config.get("fetch_size", 10000)),
        }

    def _get_bounds(self, table: str, column: str) -> tuple[int, int]:
        """
        Query min/max của partition column để Spark biết range để chia.
        Chạy 1 query nhỏ trước khi đọc toàn bộ data.
        """
        result = self.spark.read \
            .format("jdbc") \
            .options(**self._get_jdbc_options()) \
            .option("dbtable",
                    f"(SELECT MIN(CAST({column} AS BIGINT)) AS lb, "
                    f"MAX(CAST({column} AS BIGINT)) AS ub FROM {table}) bounds") \
            .load() \
            .collect()[0]

        lower = result["lb"] or 0
        upper = result["ub"] or 1
        logger.info("jdbc.bounds", table=table, column=column, lower=lower, upper=upper)
        return int(lower), int(upper)

    def read_source(self) -> DataFrame:
        """
        Đọc table từ PostgreSQL với parallel read.

        Parallel read hoạt động như sau:
        Giả sử num_partitions=4, lower=1, upper=1000000:
        Spark tạo 4 queries song song:
          Task 1: SELECT * FROM table WHERE id >= 1      AND id < 250001
          Task 2: SELECT * FROM table WHERE id >= 250001 AND id < 500001
          Task 3: SELECT * FROM table WHERE id >= 500001 AND id < 750001
          Task 4: SELECT * FROM table WHERE id >= 750001 AND id <= 1000000
        Mỗi task chạy trên 1 executor → 4 connections song song → nhanh 4x
        """
        table = self.get_source_table()
        table_key = table.replace("raw_", "")  # "raw_transactions" → "transactions"
        parallel_config = self.db_config.get("parallel_read", {}).get(table_key, {})
        num_partitions = parallel_config.get("num_partitions", 4)
        partition_col = parallel_config.get("partition_column")

        jdbc_opts = self._get_jdbc_options()
        jdbc_opts["dbtable"] = table

        if num_partitions > 1 and partition_col:
            # Parallel read — cần lower/upper bounds
            # Vấn đề: partition_column phải là NUMERIC trong Spark JDBC
            # id trong PostgreSQL là TEXT → wrap trong subquery để hash thành int
            lower, upper = self._get_bounds(table, partition_col)

            jdbc_opts.update({
                "partitionColumn": partition_col,
                "lowerBound":      str(lower),
                "upperBound":      str(upper),
                "numPartitions":   str(num_partitions),
                # Custom query để cast TEXT id sang BIGINT
                "dbtable": (
                    f"(SELECT *, CAST(REPLACE({partition_col}, '-', '0') AS BIGINT) "
                    f"AS {partition_col}_numeric FROM {table}) t"
                ),
                "partitionColumn": f"{partition_col}_numeric",
            })

            logger.info("jdbc.parallel_read",
                        table=table,
                        num_partitions=num_partitions,
                        partition_col=partition_col,
                        lower=lower, upper=upper)
        else:
            # Single partition — dùng cho bảng nhỏ (mcc_codes)
            logger.info("jdbc.single_read", table=table)

        df = self.spark.read.format("jdbc").options(**jdbc_opts).load()

        # Drop cột numeric helper nếu có
        if f"{partition_col}_numeric" in df.columns if partition_col else False:
            df = df.drop(f"{partition_col}_numeric")

        logger.info("jdbc.read_done",
                    table=table,
                    schema=str(df.schema))
        return df


# ============================================================
# Concrete ingesters — 1 class cho mỗi table
# ============================================================

class TransactionJDBCIngester(JDBCIngester):
    """Ingest raw_transactions từ PostgreSQL → HDFS raw layer."""

    def get_source_table(self) -> str:
        return self.db_config["tables"]["transactions"]  # "raw_transactions"

    def get_raw_schema(self):
        from ingestion.schema.transactions_schema import RAW_CSV_SCHEMA
        return RAW_CSV_SCHEMA  # reuse same schema — columns giống nhau

    def get_hdfs_output_path(self) -> str:
        return self.hdfs_config["tables"]["transactions"]["raw"]

    def get_partition_cols(self) -> list:
        return ["year", "month", "day"]

    def add_partition_cols(self, df: DataFrame) -> DataFrame:
        """
        Extract year/month/day từ cột 'date'.
        Dùng event time (ngày giao dịch), không phải _loaded_at hay _ingested_at.
        Lý do: late arriving data vẫn phải vào đúng partition của ngày giao dịch.
        """
        return df \
            .withColumn("_parsed_date",
                        F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("year",  F.year(F.col("_parsed_date"))) \
            .withColumn("month", F.month(F.col("_parsed_date"))) \
            .withColumn("day",   F.dayofmonth(F.col("_parsed_date"))) \
            .drop("_parsed_date")


class UserJDBCIngester(JDBCIngester):
    """Ingest raw_users từ PostgreSQL → HDFS raw layer."""

    def get_source_table(self) -> str:
        return self.db_config["tables"]["users"]

    def get_raw_schema(self):
        from ingestion.schema.users_schema import RAW_CSV_SCHEMA
        return RAW_CSV_SCHEMA

    def get_hdfs_output_path(self) -> str:
        return self.hdfs_config["tables"]["users"]["raw"]

    def get_partition_cols(self) -> list:
        return ["created_year", "created_month"]

    def add_partition_cols(self, df: DataFrame) -> DataFrame:
        """
        Users không có created_date trong dataset này.
        Dùng birth_year/birth_month làm cohort partition.
        Lý do: phân tích fraud theo age group / cohort signup.
        """
        return df \
            .withColumn("created_year",
                        F.col("birth_year").cast("int")) \
            .withColumn("created_month",
                        F.col("birth_month").cast("int"))


class CardJDBCIngester(JDBCIngester):
    """Ingest raw_cards từ PostgreSQL → HDFS raw layer."""

    def get_source_table(self) -> str:
        return self.db_config["tables"]["cards"]

    def get_raw_schema(self):
        from ingestion.schema.cards_schema import RAW_CSV_SCHEMA
        return RAW_CSV_SCHEMA

    def get_hdfs_output_path(self) -> str:
        return self.hdfs_config["tables"]["cards"]["raw"]

    def get_partition_cols(self) -> list:
        return ["card_brand_part", "expires_year_part"]

    def add_partition_cols(self, df: DataFrame) -> DataFrame:
        """
        Partition theo card_brand và expires_year.
        expires có format "MM/YYYY" → extract year.
        """
        return df \
            .withColumn("card_brand_part",
                        F.lower(F.col("card_brand"))) \
            .withColumn("expires_year_part",
                        F.split(F.col("expires"), "/").getItem(1).cast("int"))


class MCCJDBCIngester(JDBCIngester):
    """Ingest raw_mcc_codes từ PostgreSQL → HDFS raw layer. Không partition."""

    def get_source_table(self) -> str:
        return self.db_config["tables"]["mcc_codes"]

    def get_raw_schema(self):
        from ingestion.schema.mcc_schema import RAW_JSON_SCHEMA
        return RAW_JSON_SCHEMA

    def get_hdfs_output_path(self) -> str:
        return self.hdfs_config["tables"]["mcc_codes"]["raw"]

    def get_partition_cols(self) -> list:
        return []   # static dimension, không partition

    def add_partition_cols(self, df: DataFrame) -> DataFrame:
        return df   # không làm gì


class FraudLabelJDBCIngester(JDBCIngester):
    """Ingest raw_fraud_labels từ PostgreSQL → HDFS raw layer."""

    def get_source_table(self) -> str:
        return self.db_config["tables"]["fraud_labels"]

    def get_raw_schema(self):
        return None  # nhỏ, để Spark infer

    def get_hdfs_output_path(self) -> str:
        # Fraud labels không có path riêng trong hdfs config — dùng staging trực tiếp
        return self.hdfs_config["lake"]["raw"] + "/fraud_labels"

    def get_partition_cols(self) -> list:
        return []

    def add_partition_cols(self, df: DataFrame) -> DataFrame:
        return df


# ============================================================
# Entry point — chạy tất cả ingesters tuần tự
# ============================================================
if __name__ == "__main__":
    from ingestion.spark_session import get_spark_session, stop_spark_session

    spark = get_spark_session(app_name="jdbc_ingestion_all")
    try:
        ingesters = [
            MCCJDBCIngester(spark),           # nhỏ, chạy trước
            FraudLabelJDBCIngester(spark),
            UserJDBCIngester(spark),
            CardJDBCIngester(spark),
            TransactionJDBCIngester(spark),   # lớn nhất, chạy cuối
        ]

        results = []
        for ingester in ingesters:
            result = ingester.run()
            results.append(result)
            print(f"  ✓ {result['source']} → {result['row_count']} rows")

        print("\n=== Ingestion Summary ===")
        for r in results:
            print(f"  {r['source']}: {r['row_count']} rows, batch={r['batch_id']}")

    finally:
        stop_spark_session(spark)
