# gold/_gold_utils.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, col


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str, max_cores: int = 1, cores_per_executor: int = 1, memory_per_executor: str = "512m") -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name} ...")
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.cores.max", max_cores)
            .config("spark.executor.cores", cores_per_executor) 
            .config("spark.executor.memory", memory_per_executor)
            .getOrCreate())


def read_batch_from_silver(spark: SparkSession, silver_table: str, gold_table: str = None, timestamp_col: str = None) -> DataFrame:
    silver_path = f"iceberg.silver.{silver_table}"

    if gold_table is None:
        logger.info(f"Performing full load from {silver_path} ...")
        return spark.sql(f"SELECT * FROM {silver_path}")

    gold_path = f"iceberg.gold.{gold_table}"

    is_empty = spark.sql(f"SELECT 1 FROM {gold_path} LIMIT 1").count() == 0

    if is_empty or timestamp_col is None:
        logger.info(f"Performing full load from {silver_path} ...")
        query = f"SELECT * FROM {silver_path}"
    else:
        max_ingest_timestamp = spark.sql(f"SELECT MAX(ingest_timestamp) AS max_timestamp FROM {gold_path}").collect()[0][0]
        logger.info(f"Performing incremental load for {gold_path} from {silver_path} where {timestamp_col} > {max_ingest_timestamp} ...")
        query = f"SELECT * FROM {silver_path} WHERE {timestamp_col} > TIMESTAMP '{max_ingest_timestamp}'"
    
    return spark.sql(query)


def normalize_schema(df: DataFrame, schema: StructType) -> DataFrame:
    logger.info("Normalizing schema ...")
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        else:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
    silver_cols = [f.name for f in schema.fields]
    return df.select(silver_cols)