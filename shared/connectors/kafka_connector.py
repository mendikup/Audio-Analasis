from asyncio import timeout

from kafka import KafkaProducer, KafkaConsumer
import json
from shared.utils.config_loader import load_config

class Kafka_Connector:

    @staticmethod
    def get_producer():
        config = load_config()
        return KafkaProducer( bootstrap_servers=config["kafka"]["bootstrap_servers"],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))


    @staticmethod
    def get_consumer(topic: str, group_id: str):
        """
        Get a Kafka consumer with specified group_id

        Args:
            topic: The Kafka topic to consume from
            group_id: Optional group ID
        """
        config = load_config()

        return KafkaConsumer(
            topic,
            bootstrap_servers=config["kafka"]["bootstrap_servers"],
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=50000
        )
