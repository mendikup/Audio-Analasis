from services.retriever_service.src.data_loader import Files_Loader
from shared.connectors.kafka_connector import Kafka_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger

class Manager:
    def __init__(self):
        # Prepare config and producer
        config = load_config()
        self.topic = config["kafka"]["topics"]["raw_metadata"]
        self.files_loader = Files_Loader()
        self.producer = Kafka_Connector.get_producer()

    def run_all(self):
        """ scan files meta data publish to Kafka."""
        logger.info("========= PIPELINE START =========")
        docs = self.load_files_metadata()
        self._send_to_kafka(docs)
        logger.info("========= PIPELINE DONE =========")

    def load_files_metadata(self) -> list[dict]:
        """Collect metadata from file system and return it as a list of dicts."""
        data = self.files_loader.get_files_meta_data()
        logger.info(f"Loaded {len(data)} file records")
        return data

    def _send_to_kafka(self, docs: list[dict]):
        """Publish each doc to Kafka and flush at the end."""
        for doc in docs:
            try:
                self.producer.send(self.topic, doc)
            except Exception as e:
                logger.error(f"Failed to send record: {e}")
        self.producer.flush()
        logger.info("All messages flushed to Kafka")

