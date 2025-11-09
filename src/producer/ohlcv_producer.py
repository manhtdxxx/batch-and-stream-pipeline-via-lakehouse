# producer/ohlcv_producer.py

import os
import pandas as pd
import json
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer, DoubleSerializer, IntegerSerializer, SerializationContext
from confluent_kafka.serializing_producer import SerializingProducer
import time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def create_topics(kafka_server: str, topic_name: str, n_partitions: int = 1):
    admin_client = AdminClient({"bootstrap.servers": f"{kafka_server}"})
    existing_topics = admin_client.list_topics().topics.keys()

    if topic_name not in existing_topics:
        topic = NewTopic(topic=topic_name, num_partitions=n_partitions, replication_factor=1)
        try:
            admin_client.create_topics([topic])
            logger.info(f"Created topic '{topic_name}' with {n_partitions} partitions.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
    else:
        logger.info(f"Topic '{topic_name}' already exists.")


def _delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def produce_ohlcv_to_kafka(df: pd.DataFrame, kafka_server: str, topic_name: str):
    producer = SerializingProducer({
        "bootstrap.servers": kafka_server,
        "key.serializer": StringSerializer('utf-8'),
        "value.serializer": StringSerializer('utf-8')
    })

    for idx, row in df.iterrows():
        key = f'{str(row["symbol"])}_{str(row["time"])}'
        value = row.to_dict()
        value_str = json.dumps(value)
        try:
            producer.produce(topic=topic_name, key=key, value=value_str, on_delivery=_delivery_report)
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")
        
        producer.poll(0)
        time.sleep(1/30)

    producer.flush()
    logger.info("All messages have been produced.")


if __name__ == "__main__":
    data_dir = "/opt/airflow/data"
    file_path = os.path.join(data_dir, "ohlcv_1m.csv")
    df = pd.read_csv(file_path)

    kafka_server = "kafka:29092"
    topic_name = "ohlcv_1m"

    create_topics(kafka_server, topic_name)
    produce_ohlcv_to_kafka(df, kafka_server, topic_name)