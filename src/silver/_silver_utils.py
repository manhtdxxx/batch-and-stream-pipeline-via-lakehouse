# silver/_silver_utils.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, when, trim, current_timestamp, row_number, desc
from pyspark.sql.types import StructType, IntegerType, TimestampType
from pyspark.sql.window import Window
from typing import Callable
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

# -------- Read --------
def read_stream_from_bronze(spark: SparkSession, bronze_table: str) -> DataFrame:
    logger.info(f"Reading bronze stream from table: {bronze_table} ...")
    df = spark.readStream.format("iceberg").load(f"iceberg.bronze.{bronze_table}")
    return df


def read_batch_from_bronze(spark: SparkSession, bronze_table: str, silver_table: str, timestamp_col: str = "ingest_timestamp") -> DataFrame:
    logger.info(f"Starting incremental load from {bronze_table} to {silver_table} ...")
    bronze_path = f"iceberg.bronze.{bronze_table}"
    silver_path = f"iceberg.silver.{silver_table}"

    is_empty = spark.sql(f"SELECT 1 FROM {silver_path} LIMIT 1").count() == 0

    if is_empty:
        logger.info(f"Silver Table {silver_table} is empty. Performing full load from Bronze Table {bronze_table} ...")
        query = f"SELECT * FROM {bronze_path}"
    else:
        max_ingest_timestamp = spark.sql(f"SELECT MAX(ingest_timestamp) AS max_timestamp FROM {silver_path}").collect()[0][0]
        logger.info(f"Performing incremental load for Silver Table {silver_table} from Bronze Table {bronze_table} where {timestamp_col} > {max_ingest_timestamp} ...")
        query = f"SELECT * FROM {bronze_path} WHERE {timestamp_col} > TIMESTAMP '{max_ingest_timestamp}'"
    
    return spark.sql(query)


# -------- Normalizing Schema / Rename Columns --------
def normalize_schema(df: DataFrame, schema: StructType) -> DataFrame:
    logger.info("Normalizing schema ...")
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        else:
            # if bronze lacks col compared to schema, then add None value
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
    # if bronze add new col(s)
    silver_cols = [f.name for f in schema.fields]
    remaining_cols = [c for c in df.columns if c not in silver_cols]
    return df.select(silver_cols + remaining_cols)


def rename_cols(df: DataFrame, rename_map: dict) -> DataFrame:
    logger.info(f"Renaming columns ...")
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


# -------- Cleaning --------
def handle_string(df: DataFrame, string_cols: list[str]) -> DataFrame:
    logger.info(f"Handling string columns: {string_cols} ...")
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    return df


def handle_numeric(df: DataFrame, numeric_cols: list[str]) -> DataFrame:
    logger.info(f"Handling numeric columns: {numeric_cols} ...")
    for c in numeric_cols:
        df = df.withColumn(c, when(col(c) <= 0, None).otherwise(col(c)))
    return df


def handle_null(df: DataFrame, dropna_cols: list[str] = None, fill_map: dict = None) -> DataFrame:
    if dropna_cols:
        logger.info(f"Handling null values for columns: {dropna_cols} ...")
        df = df.dropna(subset=dropna_cols)
    if fill_map:
        logger.info(f"Filling null values: {fill_map}")
        df = df.fillna(fill_map)
    return df


def deduplicate(df: DataFrame, key_cols: list[str], condition_col: str) -> DataFrame:
    logger.info(f"Deduplicating DataFrame based on keys: {key_cols} ...")
    window = Window.partitionBy(*key_cols).orderBy(desc(condition_col))
    df = df.withColumn("rank", row_number().over(window))
    return df.filter(col("rank") == 1).drop("rank")


# -------- SCD Type 2 / Close old records & Insert new records --------
def _add_scd2_cols(df: DataFrame) -> DataFrame:
    logger.info("Adding SCD2 columns (start_timestamp, end_timestamp, is_current) ...")
    return (df.withColumn("start_timestamp", current_timestamp())
              .withColumn("end_timestamp", lit(None).cast(TimestampType()))
              .withColumn("is_current", lit(1).cast(IntegerType())))
# it would be better to have col "updated_at", since I dont have it, I will use current_timestamp()


def merge_scd2(df: DataFrame, silver_table: str, key_cols: list[str], tracked_cols: list[str], spark: SparkSession):
    df = _add_scd2_cols(df)
    df.createOrReplaceTempView("source")
    target = f"iceberg.silver.{silver_table}"

    matched_conditions = " AND ".join([f"source.{c} = target.{c}" for c in key_cols])
    updated_conditions = " OR ".join([f"NOT (source.{c} <=> target.{c})" for c in tracked_cols])
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join([f"source.{c}" for c in df.columns])

    is_empty = spark.sql(f"SELECT 1 FROM {target} LIMIT 1").count() == 0

    if is_empty:
        insert_stmt = f"""
            INSERT INTO {target} ({insert_cols}) 
            SELECT {insert_vals} FROM source
        """
        logger.info(f"Inserting data into empty silver table: {silver_table} ...")
        spark.sql(insert_stmt)
    else:
        update_stmt = f"""
            MERGE INTO {target} AS target
            USING source
            ON {matched_conditions} AND target.is_current = 1
            WHEN MATCHED AND ({updated_conditions}) THEN
                UPDATE SET end_timestamp = CURRENT_TIMESTAMP(), is_current = 0
        """
        insert_stmt = f"""
            MERGE INTO {target} AS target
            USING source AS source
            ON {matched_conditions} AND target.is_current = 1
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        logger.info(f"Updating old records in Silver Table: {silver_table} ...")
        spark.sql(update_stmt)
        logger.info(f"Inserting new records into Silver Table: {silver_table} ...")
        spark.sql(insert_stmt)


# -------- SCD1 / Upsert Overwrite --------
def merge_scd1(df: DataFrame, silver_table: str, key_cols: list[str], spark: SparkSession):
    df.createOrReplaceTempView("source")
    target = f"iceberg.silver.{silver_table}"

    matched_conditions = " AND ".join([f"source.{c} = target.{c}" for c in key_cols])
    update_clause = ", ".join([f"target.{c} = source.{c}" for c in df.columns])
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join([f"source.{c}" for c in df.columns])

    is_empty = spark.sql(f"SELECT 1 FROM {target} LIMIT 1").count() == 0

    if is_empty:
        insert_stmt = f"""
            INSERT INTO {target} ({insert_cols})
            SELECT {insert_vals} FROM source
        """
        logger.info(f"Inserting data into empty silver table: {silver_table} ...")
        spark.sql(insert_stmt)
    else:
        upsert_stmt = f"""
            MERGE INTO {target} AS target
            USING source
            ON {matched_conditions}
            WHEN MATCHED THEN
              UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        logger.info(f"Upserting into Silver Fact Table: {silver_table} ...")
        spark.sql(upsert_stmt)