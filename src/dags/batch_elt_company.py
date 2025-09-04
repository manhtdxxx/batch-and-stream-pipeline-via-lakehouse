# dags/batch_elt_company.py

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
import os


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


SPARK_ENV = (
    "export JAVA_HOME=/opt/bitnami/java && "
    "export PATH=$JAVA_HOME/bin:$PATH && "
    "export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3"
)
SPARK_SUBMIT = "/opt/bitnami/spark/bin/spark-submit"
SPARK_APP = "/opt/spark/app"


with DAG(
    dag_id="batch_elt_company",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    load_to_bronze = SSHOperator(
        task_id="load_to_bronze",
        ssh_conn_id="ssh_spark",
        command=(
            f"bash -c '{SPARK_ENV} && {SPARK_SUBMIT} {os.path.join(SPARK_APP, 'bronze', 'raw_company.py')}'"
        ),
    )

    process_from_bronze_to_silver = SSHOperator(
        task_id="process_from_bronze_to_silver",
        ssh_conn_id="ssh_spark",
        command=(
            f"bash -c '{SPARK_ENV} && {SPARK_SUBMIT} {os.path.join(SPARK_APP, 'silver', 'processed_company.py')}'"
        ),
    )
    load_to_bronze >> process_from_bronze_to_silver
