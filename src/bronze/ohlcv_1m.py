# bronze/ohlcv_1m.py

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from _bronze_utils import (
    create_spark_session, read_stream, add_metadata, write_stream_to_bronze,
)


def get_json_schema() -> StructType:
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("time", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])


if __name__ == "__main__":
    KAFKA_SERVER = "kafka:29092"
    TOPIC = "ohlcv_1m"
    BRONZE_TABLE = TOPIC
    CHECKPOINT_PATH = "./checkpoint/ohlcv_1m"

    spark = create_spark_session("StreamLoadPriceVolumeDataFromKafkaToBronze")

    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    df = read_stream(spark, KAFKA_SERVER, TOPIC)

    # Deserialize when reading from kafka
    df = df.selectExpr("CAST(value AS STRING) as value_str")
    df = df.select(from_json(col("value_str"), schema=get_json_schema()).alias("data")) \
            .select("data.*")


    df = df.withColumnRenamed("time", "event_timestamp")

    partition_cols = [("event_year", year), ("event_month", month), ("event_day", dayofmonth)]
    col_names = []
    for col_name, func in partition_cols:
        df = df.withColumn(col_name, func(col("event_timestamp")))
        col_names.append(col_name)

    df = add_metadata(df, ingest_year=False, ingest_month=False, ingest_day=False, batch_id=False)

    write_stream_to_bronze(df, table_name=BRONZE_TABLE, checkpoint_path=CHECKPOINT_PATH, processingTime="5 seconds", partition_cols=col_names)
