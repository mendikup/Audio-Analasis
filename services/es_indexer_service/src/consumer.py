import time
from dal.elastic_dal import Elastic_DAL
from shared.connectors.kafka_connector import Kafka_Connector
from shared.connectors.elastic_connector import Elastic_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger


class ESIndexer:
    def __init__(self):
        config = load_config()
        self.metadata_topic = config["kafka"]["topics"]["raw_metadata"]
        self.transcription_topic = config["kafka"]["topics"]["transcription_ready"]

        # Create two consumers, one to consume the Metadata messages
        # and the other for The transcribed messages from the Transcription service
        self.metadata_consumer = Kafka_Connector.get_consumer(
            self.metadata_topic,
            group_id="es-indexer-metadata-group"
        )
        self.transcription_consumer = Kafka_Connector.get_consumer(
            self.transcription_topic,
            group_id="es-indexer-transcription-group"
        )

        self.dal = Elastic_DAL()
        self.index_name = "files_metadata"

    def run(self):
        logger.info("ESIndexer started and waiting for messages...")
        while True:
            try:
                # Handle metadata messages
                self._process_metadata_messages()
                # Handle transcription messages
                self._process_transcription_messages()

            except Exception as e:
                logger.error(f"Consumer loop error: {e}. Restarting consumers in 1s...")
                self._restart_consumers()
                time.sleep(1)
                continue

    def _process_metadata_messages(self):
        """Process metadata messages"""
        for msg in self.metadata_consumer:
            doc = msg.value
            doc_id = doc.get("absolute_path")
            if not doc_id:
                logger.warning("Skipping metadata message without absolute_path")
                continue

            try:
                self.dal.index_or_update_doc(index_name=self.index_name, doc_id=doc_id, doc=doc)
                logger.info(f"Indexed metadata for: {doc_id}")
            except Exception as e:
                logger.error(f"Failed to index metadata document to ES: {e}")

    def _process_transcription_messages(self):
        """Process transcription messages"""
        for msg in self.transcription_consumer:
            doc = msg.value
            if doc.get("message_type") != "transcription":
                continue

            doc_id = doc.get("absolute_path")
            content = doc.get("content")

            if not doc_id or not content:
                logger.warning("Skipping transcription message without absolute_path or content")
                continue

            try:
                # Update existing document with transcription content
                update_doc = {"content": content}
                self.dal.index_or_update_doc(index_name=self.index_name, doc_id=doc_id, doc=update_doc)
                logger.info(f"Updated document with transcription: {doc_id}")
            except Exception as e:
                logger.error(f"Failed to update document with transcription: {e}")

    def _restart_consumers(self):
        """Restart both consumers"""
        try:
            self.metadata_consumer.close()
            self.transcription_consumer.close()
        except Exception:
            pass

        config = load_config()
        self.metadata_consumer = Kafka_Connector.get_consumer(config["kafka"]["topics"]["raw_metadata"])
        self.transcription_consumer = Kafka_Connector.get_consumer(config["kafka"]["topics"]["transcription_ready"])


