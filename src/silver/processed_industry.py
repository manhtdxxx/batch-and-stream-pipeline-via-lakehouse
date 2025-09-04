# silver/processed_industry.py

import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.dataframe import DataFrame
from _silver_utils import (
    create_spark_session, read_batch_from_bronze, normalize_schema, rename_cols, 
    handle_string, handle_numeric, handle_null, deduplicate, merge_scd2
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_schema() -> StructType:
    return StructType([
        StructField("icb_code", StringType(), True),
        StructField("level", IntegerType(), True),
        StructField("icb_name", StringType(), True),
        StructField("en_icb_name", StringType(), True),
        StructField("batch_id", StringType(), False),
        StructField("ingest_timestamp", TimestampType(), False),
        StructField("ingest_year", IntegerType(), False),
        StructField("ingest_month", IntegerType(), False)
    ])


def get_cols(col_dtype: str) -> list[str]:
    if col_dtype == "string":
        return ["icb_code", "icb_name", "en_icb_name"]
    elif col_dtype == "numeric":
        return ["level"]
    

def get_keys() -> list[str]:
    return ["icb_code"]


def get_tracked_cols() -> list[str]:
    return ["level", "icb_name", "en_icb_name"]


def process_df(df: DataFrame) -> DataFrame:
    df = normalize_schema(df, schema=get_schema())

    df = handle_string(df, string_cols=get_cols("string"))
    df = handle_numeric(df, numeric_cols=get_cols("numeric"))
    df = handle_null(df, dropna_cols=df.columns)

    if df.rdd.isEmpty():
        logger.info("No data left after handle_null. Exiting.")
        return None
    
    df = deduplicate(df, key_cols=get_keys(), condition_col="ingest_timestamp")

    if df.rdd.isEmpty():
        logger.info("No data left after deduplicate. Exiting.")
        return None

    return df


if __name__ == "__main__":
    BRONZE_TABLE = "raw_industry"
    SILVER_TABLE = "processed_industry"

    spark = create_spark_session("BatchProcessIndustryDataFromBronzeToSilver")
    df = read_batch_from_bronze(spark, bronze_table=BRONZE_TABLE, silver_table=SILVER_TABLE, timestamp_col="ingest_timestamp")

    if df.rdd.isEmpty():
        logger.info(f"No new data to process from {BRONZE_TABLE}. Exiting.")
    else:
        df = process_df(df)
        if df is not None:
            logger.info(f"Starting to write batch to Silver Table {SILVER_TABLE} with {df.count()} records ...")
            merge_scd2(df, silver_table=SILVER_TABLE, key_cols=get_keys(), tracked_cols=get_tracked_cols(), spark=df.sparkSession)
            logger.info(f"Batch writing completed.")
