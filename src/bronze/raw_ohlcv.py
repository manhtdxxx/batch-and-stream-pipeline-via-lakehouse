# bronze/raw_ohlcv.py

from pyspark.sql import SparkSession
from _bronze_utils import (
    create_spark_session,
    read_stream,
    add_metadata,
    write_stream_to_bronze,
)


if __name__ == "__main__":
    KAFKA_SERVER = "kafka:29092"
    TOPIC = "ohlcv_1d"
    BRONZE_TABLE = "raw_ohlcv"
    CHECKPOINT_PATH = "./bronze_checkpoint/ohlcv"

    spark = create_spark_session("StreamLoadPriceVolumeDataFromKafkaToBronze")
    df = read_stream(spark, KAFKA_SERVER, TOPIC)

    # deserialize when reading from kafka
    df = df.selectExpr("CAST(value AS STRING) as value_str")

    df = add_metadata(df, with_batch_id=False)

    write_stream_to_bronze(df, table_name=BRONZE_TABLE, checkpoint_path=CHECKPOINT_PATH, processingTime="30 seconds")
