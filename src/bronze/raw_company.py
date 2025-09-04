# bronze/raw_company.py

from _bronze_utils import create_spark_session, read_csv, add_metadata, write_batch_to_bronze
import os


if __name__ == "__main__":
    data_dir = "/opt/spark/data"
    file_path = os.path.join(data_dir, "company.csv")
    BRONZE_TABLE = "raw_company"

    spark = create_spark_session(app_name="BatchLoadCompanyDataToBronze")
    df = read_csv(spark, file_path)
    df_with_metadata = add_metadata(df)
    write_batch_to_bronze(df_with_metadata, table_name=BRONZE_TABLE)
