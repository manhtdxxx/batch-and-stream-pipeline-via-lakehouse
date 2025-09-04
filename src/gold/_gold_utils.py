# gold/_gold_utils.py

from pyspark.sql import SparkSession
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str, max_cores: int = 1, cores_per_executor: int = 1, memory_per_executor: str = "512m") -> SparkSession:
    logger.info(f"Creating SparkSession: {app_name} ...")
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.cores.max", max_cores)
            .config("spark.executor.cores", cores_per_executor) 
            .config("spark.executor.memory", memory_per_executor)
            .getOrCreate())