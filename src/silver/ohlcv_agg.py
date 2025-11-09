# silver/ohlcv_agg.py

import logging
import os
from pyspark.sql.functions import col, max, min, sum, first, last, window, hour, lit
from pyspark.sql.dataframe import DataFrame
from _silver_utils import create_spark_session, read_stream_from_bronze


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def agg_ohlcv(df: DataFrame, window_duration: str, watermark_duration: str) -> DataFrame:
    logger.info(f"Creating {window_duration}-minute OHLCV DataFrame with watermark={watermark_duration}")

    df_agg = df.withWatermark("event_timestamp", watermark_duration) \
                .groupBy("symbol", window("event_timestamp", window_duration).alias("w")) \
                .agg(
                    first("open").alias("open"),
                    max("high").alias("high"),
                    min("low").alias("low"),
                    last("close").alias("close"),
                    sum("volume").alias("volume")
                )
    
    df_agg = df_agg.select(
        "symbol", col("w.start").alias("start_window"), col("w.end").alias("end_window"),
        "open", "high", "low", "close", "volume"
    )

    # df_agg = df_agg.withColumn("timeframe", lit("15m"))
    df_agg = df_agg.filter(hour(col("end_window")) <= 14)

    return df_agg


def write_stream(df: DataFrame, silver_table: str, checkpoint_path: str, trigger_interval: str):
    try:
        logger.info(f"Starting stream to table '{silver_table}' with trigger interval '{trigger_interval}'")
        query = (
            df.writeStream
              .format("iceberg")
              .outputMode("append")
              .option("checkpointLocation", checkpoint_path)
              .trigger(processingTime=trigger_interval)
              .toTable(f"iceberg.silver.{silver_table}")
        )
        query.awaitTermination()
    except Exception as e:
        logger.exception(f"Streaming failed for table {silver_table}: {e}")
        raise


if __name__ == "__main__":
    BRONZE_TABLE = "ohlcv_1m"
    SILVER_TABLE = "ohlcv_agg"
    CHECKPOINT_PATH = "./checkpoint/ohlcv_agg"

    spark = create_spark_session("StreamProcessPriceVolumeDataFromBonzeToSilver")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    df = read_stream_from_bronze(spark, bronze_table=BRONZE_TABLE)

    WATERMARK_DURATION = "1 minutes"
    WINDOW_DURATION = "15 minutes"
    df_15m = agg_ohlcv(df, WINDOW_DURATION, WATERMARK_DURATION)

    TRIGGER_INTERVAL = "5 seconds"
    write_stream(df_15m, silver_table=SILVER_TABLE, checkpoint_path=CHECKPOINT_PATH, trigger_interval=TRIGGER_INTERVAL)
