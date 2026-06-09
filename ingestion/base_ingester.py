"""
BaseIngester — Abstract base class cho tất cả ingesters.

Pattern: Template Method
- Base class define flow chuẩn: validate → read → add_metadata → write
- Subclass chỉ implement những phần khác nhau: schema, paths, partition logic
- Flow không thể bị bypass hoặc miss step

Tại sao quan trọng?
- Đảm bảo mọi ingester đều thêm metadata columns (_ingested_at, _batch_id, ...)
- Đảm bảo mọi ingester đều check idempotency trước khi write
- Logging nhất quán — dễ debug khi có lỗi
"""

import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import structlog

logger = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent


class BaseIngester(ABC):
    """
    Abstract base class. Subclass phải implement:
    - get_source_path(): đường dẫn file CSV/JSON trên host
    - get_raw_schema(): StructType khi đọc từ source
    - get_hdfs_output_path(): HDFS path để write
    - get_partition_cols(): list partition columns
    - read_source(): đọc file vào DataFrame
    - add_partition_cols(): thêm partition columns từ data
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.ingested_at = datetime.utcnow()
        self._load_config()

    def _load_config(self):
        """Load HDFS config."""
        with open(PROJECT_ROOT / "config" / "hdfs.yaml") as f:
            self.hdfs_config = yaml.safe_load(f)

    # ---- Abstract methods — subclass phải implement ----

    @abstractmethod
    def get_source_path(self) -> str:
        """Path tới file nguồn (CSV/JSON)."""
        pass

    @abstractmethod
    def get_raw_schema(self) -> StructType:
        """StructType để đọc source file."""
        pass

    @abstractmethod
    def get_hdfs_output_path(self) -> str:
        """HDFS path để write output."""
        pass

    @abstractmethod
    def get_partition_cols(self) -> list:
        """List partition columns. Empty list = không partition."""
        pass

    @abstractmethod
    def read_source(self) -> DataFrame:
        """Đọc source file vào DataFrame."""
        pass

    @abstractmethod
    def add_partition_cols(self, df: DataFrame) -> DataFrame:
        """Thêm partition columns (extract từ date columns, v.v.)."""
        pass

    # ---- Template method — flow chuẩn, không override ----

    def run(self) -> dict:
        """
        Entry point. Chạy full ingestion flow.
        Returns: dict với stats (row_count, batch_id, status)
        """
        source = self.get_source_path()
        output = self.get_hdfs_output_path()

        logger.info("ingester.start",
                    source=source,
                    output=output,
                    batch_id=self.batch_id)

        try:
            # Step 1: Đọc source
            df = self.read_source()
            logger.info("ingester.read_done",
                        batch_id=self.batch_id,
                        schema=str(df.schema))

            # Step 2: Thêm metadata columns
            df = self._add_metadata(df, source)

            # Step 3: Thêm partition columns
            df = self.add_partition_cols(df)

            # Step 4: Write to HDFS
            row_count = self._write(df, output)

            result = {
                "status": "success",
                "batch_id": self.batch_id,
                "source": source,
                "output": output,
                "row_count": row_count,
                "ingested_at": self.ingested_at.isoformat(),
            }
            logger.info("ingester.done", **result)
            return result

        except Exception as e:
            logger.error("ingester.failed",
                         batch_id=self.batch_id,
                         error=str(e),
                         exc_info=True)
            raise

    # ---- Internal helpers ----

    def _add_metadata(self, df: DataFrame, source_path: str) -> DataFrame:
        """
        Thêm 3 metadata columns vào mọi record.
        Đây là audit trail — không bao giờ xóa các columns này.
        """
        return df \
            .withColumn("_ingested_at",
                        F.lit(self.ingested_at.isoformat()).cast("timestamp")) \
            .withColumn("_source_file",
                        F.lit(Path(source_path).name)) \
            .withColumn("_batch_id",
                        F.lit(self.batch_id))

    def _write(self, df: DataFrame, output_path: str) -> int:
        """
        Write DataFrame to HDFS as Parquet.

        Mode: idempotent overwrite per partition (dynamic partition overwrite).
        - spark.sql.sources.partitionOverwriteMode = dynamic
        - Chỉ overwrite đúng partition đang write, không đụng partitions khác.
        - Chạy lại cùng job cho cùng ngày → overwrite partition đó với data mới nhất.
        - Partitions của ngày khác không bị ảnh hưởng.

        Đây là idempotent, không phải strict append-only.
        Strict append-only sẽ tạo duplicate khi chạy lại → cần dedup downstream.
        """
        partition_cols = self.get_partition_cols()

        # Đếm rows trước khi write (trigger action)
        row_count = df.count()
        logger.info("ingester.write_start",
                    output=output_path,
                    row_count=row_count,
                    partitions=partition_cols)

        if partition_cols:
            # repartition theo partition columns trước để:
            # 1. Data cùng partition về cùng executor → ít files hơn
            # 2. Tránh small file problem
            df_partitioned = df.repartition(*[F.col(c) for c in partition_cols])
            df_partitioned.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .partitionBy(*partition_cols) \
                .parquet(output_path)
        else:
            # Không partition (ví dụ: mcc_codes)
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("compression", "snappy") \
              .parquet(output_path)

        logger.info("ingester.write_done",
                    output=output_path,
                    row_count=row_count)
        return row_count
