# bronze/_bronze_utils.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, year, month
import uuid
import logging


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


def read_csv(spark: SparkSession, file_path: str) -> DataFrame:
    logger.info(f"Reading CSV file: {file_path} ...")
    return spark.read.csv(file_path, header=True, inferSchema=False)


def read_stream(spark: SparkSession, kafka_server: str, topic: str):
    logger.info(f"Starting to read stream from Kafka topic '{topic}' on server '{kafka_server}' ...")
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load())


def add_metadata(df: DataFrame, with_batch_id: bool = True) -> DataFrame:
    logger.info("Added metadata: ingest_timestamp, ingest_year/month, ...")
    df = (df.withColumn("ingest_timestamp", current_timestamp())
            .withColumn("ingest_year", year(current_timestamp()))
            .withColumn("ingest_month", month(current_timestamp())))
    if with_batch_id:
        batch_id = str(uuid.uuid4())
        df = df.withColumn("batch_id", lit(batch_id))
    return df


def write_batch_to_bronze(df: DataFrame, table_name: str) -> None:
    partition_cols = ["ingest_year", "ingest_month"]
    logger.info(f"Writing Batch to bronze table: {table_name} ...")
    (df.write.format("iceberg")
            .mode("append")
            .partitionBy(*partition_cols)
            .saveAsTable(f"iceberg.bronze.{table_name}"))
    logger.info("Write successful!")


def write_stream_to_bronze(df: DataFrame, table_name: str, checkpoint_path: str, processingTime: str = "5 seconds") -> None:
    partition_cols = ["ingest_year", "ingest_month"]
    logger.info(f"Writing Stream to bronze table: {table_name} ...")
    query = (df.writeStream.format("iceberg")
             .outputMode("append")
             .option("checkpointLocation", checkpoint_path)
             .trigger(processingTime=processingTime)
             .partitionBy(*partition_cols)
             .toTable(f"iceberg.bronze.{table_name}"))
    try:
        logger.info("Stream is running, awaiting termination...")
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stream terminated by user (KeyboardInterrupt).")
    finally:
        logger.info("Stream has stopped.")