CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;


CREATE TABLE IF NOT EXISTS iceberg.silver.processed_company (
    symbol VARCHAR,
    company_name VARCHAR,
    icb_code_1 VARCHAR,
    icb_code_2 VARCHAR,
    icb_code_3 VARCHAR,
    icb_code_4 VARCHAR,
    issued_shares BIGINT,
    start_timestamp TIMESTAMP WITH TIME ZONE,
    end_timestamp TIMESTAMP WITH TIME ZONE,
    is_current INT,
    batch_id VARCHAR,
    ingest_timestamp TIMESTAMP WITH TIME ZONE,
    ingest_year INT,
    ingest_month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingest_year', 'ingest_month']
);


CREATE TABLE IF NOT EXISTS iceberg.silver.processed_industry (
    icb_code VARCHAR,
    level INT,
    icb_name VARCHAR,
    en_icb_name VARCHAR,
    batch_id VARCHAR,
    ingest_timestamp TIMESTAMP WITH TIME ZONE,
    ingest_year INT,
    ingest_month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingest_year', 'ingest_month']
);


CREATE TABLE IF NOT EXISTS iceberg.gold.dim_company (
    symbol VARCHAR,
    company_name VARCHAR,
    icb_name_1 VARCHAR,
    icb_name_2 VARCHAR,
    icb_name_3 VARCHAR,
    icb_name_4 VARCHAR,
    issued_shares BIGINT,
    ingest_timestamp TIMESTAMP WITH TIME ZONE,
    ingest_year INT,
    ingest_month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingest_year', 'ingest_month']
);



