# silver/dim_company.py

import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import col, broadcast
from pyspark.sql.dataframe import DataFrame
from _gold_utils import create_spark_session, read_batch_from_silver, normalize_schema


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_schema() -> StructType:
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("icb_name_1", StringType(), True),
        StructField("icb_name_2", StringType(), True),
        StructField("icb_name_3", StringType(), True),
        StructField("icb_name_4", StringType(), True),
        StructField("issued_shares", LongType(), True),
        StructField("ingest_timestamp", TimestampType(), False),
        StructField("ingest_year", IntegerType(), False),
        StructField("ingest_month", IntegerType(), False)
    ])


def join_company_with_industry(df_company: DataFrame, df_industry: DataFrame) -> DataFrame:
    logger.info("Broadcasting industry lookup table ...")
    df_industry_broadcasted = broadcast(df_industry.select("icb_code", col("en_icb_name").alias("icb_name")))

    for i in range(1, 5):
        code_col = f"icb_code_{i}"
        name_col = f"icb_name_{i}"
        logger.info(f"Joining on {code_col} ...")
        df_company = df_company.join(df_industry_broadcasted, df_company[code_col] == col("icb_code"), how="left") \
                                .drop(code_col, "icb_code") \
                                .withColumnRenamed("icb_name", name_col)
    return df_company


if __name__ == "__main__":
    SILVER_COMPANY = "processed_company"
    SILVER_INDUSTRY = "processed_industry"
    GOLD_COMPANY = "dim_company"

    spark = create_spark_session(app_name="JoinCompanyAndIndustryToDimCompany")
    df_company = read_batch_from_silver(spark, silver_table=SILVER_COMPANY, gold_table=GOLD_COMPANY, timestamp_col="ingest_timestamp")

    if df_company.rdd.isEmpty():
        logger.warning("Source company DataFrame is empty. Skipping job.")
    else:
        df_industry = read_batch_from_silver(spark, silver_table=SILVER_INDUSTRY)

        df_joined = join_company_with_industry(df_company, df_industry)
        df_joined = normalize_schema(df_joined, get_schema())

        logger.info(f"Writing result to iceberg.gold.{GOLD_COMPANY} ...")
        df_joined.write.format("iceberg") \
                        .mode("append") \
                        .saveAsTable(f"iceberg.gold.{GOLD_COMPANY}")
        logger.info("Job completed successfully.")