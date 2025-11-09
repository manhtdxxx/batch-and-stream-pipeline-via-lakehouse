# bronze/raw_industry.py

from _bronze_utils import create_spark_session, read_csv, add_metadata, write_batch_to_bronze
import os


if __name__ == "__main__":
    data_dir = "/opt/spark/data"
    file_path = os.path.join(data_dir, "industry.csv")
    BRONZE_TABLE = "raw_industry"

    spark = create_spark_session(app_name="BatchLoadIndustryDataToBronze")
    df = read_csv(spark, file_path)
    df_with_metadata = add_metadata(df, ingest_year=True, ingest_month=True, ingest_day=False, batch_id=True)
    write_batch_to_bronze(df_with_metadata, table_name=BRONZE_TABLE)
