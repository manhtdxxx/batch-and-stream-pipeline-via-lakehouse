# silver/processed_ohlcv.py

import logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.dataframe import DataFrame
from _silver_utils import create_spark_session, read_stream_from_bronze, handle_numeric, handle_string, handle_null, deduplicate, merge_scd1


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_json_schema() -> StructType:
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("time", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ])


def get_cols(col_dtype: str) -> list[str]:
    if col_dtype == "string":
        return ["symbol"]
    elif col_dtype == "numeric":
        return ["open", "high", "low", "close", "volume"]


def get_keys() -> list[str]:
    return ["symbol", "time"]


def process_df(df: DataFrame) -> DataFrame:
    metadata_cols = [c for c in df.columns if c != "value_str"]
    df = df.select(from_json(col("value_str"), schema=get_json_schema()).alias("data"), *metadata_cols) \
        .select("data.*", *metadata_cols)
    
    df = df.withColumn("ingest_timestamp", col("ingest_timestamp").cast(TimestampType())) \
           .withColumn("ingest_year", col("ingest_year").cast(IntegerType())) \
           .withColumn("ingest_month", col("ingest_month").cast(IntegerType()))
    
    df = handle_string(df, get_cols("string"))
    df = handle_numeric(df, get_cols("numeric"))

    df = handle_null(df, dropna_cols=df.columns)
    df = deduplicate(df, key_cols=get_keys(), condition_col="ingest_timestamp")

    return df



if __name__ == "__main__":
    BRONZE_TABLE = "raw_ohlcv"
    SILVER_TABLE = "processed_ohlcv"
    CHECKPOINT_PATH = "./silver_checkpoint/ohlcv"

    spark = create_spark_session("StreamProcessPriceVolumeDataFromBonzeToSilver")
    df = read_stream_from_bronze(spark, bronze_table=BRONZE_TABLE)

    def process_func(batch_df: DataFrame, batch_id: int):
        df = process_df(batch_df)
        logger.info(f"Starting to write stream to Silver Table {SILVER_TABLE} with {df.count()} records ...")
        merge_scd1(df, silver_table=SILVER_TABLE, key_cols=get_keys(), spark=df.sparkSession)
        logger.info(f"Finished processing batch_id={batch_id}.")

    query = df.writeStream \
              .foreachBatch(process_func) \
              .option("checkpointLocation", CHECKPOINT_PATH) \
              .trigger(processingTime="30 seconds") \
              .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stream terminated by user.")
    finally:
        logger.info("Stream terminated.")




