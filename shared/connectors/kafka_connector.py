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
    def get_consumer(topic: str):
        config = load_config()
        return KafkaConsumer(topic, bootstrap_servers=config["kafka"]["bootstrap_servers"],
                                    group_id="raw-metadata",
                                    value_deserializer=lambda v: json.loads(v.decode("ascii")),
                                    consumer_timeout_ms=10000)
