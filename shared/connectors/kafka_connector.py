from kafka import KafkaProducer, KafkaConsumer
import json
from shared.utils.config_loader import load_config
from shared.utils.logger import logger


class Kafka_Connector:
    @staticmethod
    def get_producer() -> KafkaProducer:
        try:
            config = load_config()
            servers = config["kafka"]["bootstrap_servers"]
            logger.info("Creating Kafka producer")
            return KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as e:
            logger.error(f"Error creating Kafka producer: {e}")
            raise

    @staticmethod
    def get_consumer(topic: str, group_id: str):
        """
        Get a Kafka consumer with specified group_id
        """
        try:
            config = load_config()
            servers = config["kafka"]["bootstrap_servers"]
            timeout_ms = config["kafka"].get("consumer_timeout_ms", 1000)
            logger.info(f"Creating Kafka consumer for topic: {topic}")
            return KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                consumer_timeout_ms=timeout_ms
            )
        except Exception as e:
            logger.error(f"Error creating Kafka consumer: {e}")
            raise