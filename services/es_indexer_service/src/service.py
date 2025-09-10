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
        self.transcription_topic = config["kafka"]["topics"]["transcribed_content"]
        self.metadata_group = "es-indexer-metadata-group"
        self.transcription_group = "es-indexer-transcription-group"
        self.batch_size = config["kafka"].get("consumer_batch_size", 50)
        self.timeout_ms = config["kafka"].get("consumer_timeout_ms", 1000)

        # Consumers with stable group IDs and manual commit
        self.metadata_consumer = Kafka_Connector.get_consumer(
            self.metadata_topic, group_id=self.metadata_group
        )
        self.transcription_consumer = Kafka_Connector.get_consumer(
            self.transcription_topic, group_id=self.transcription_group
        )

        self.dal = Elastic_DAL()
        # Index name loaded from configuration
        self.index_name = config["elasticsearch"]["indexes"]["files_metadata"]

    def run(self):
        logger.info("ESIndexer started and waiting for messages...")
        while True:
            try:
                self._process_metadata_messages()
                self._process_transcription_messages()
            except Exception as e:
                logger.error(f"Consumer loop error: {e}. Restarting consumers in 1s...")
                self._restart_consumers()
                time.sleep(1)
                continue

    def _process_metadata_messages(self):
        """Process metadata messages"""
        records = self.metadata_consumer.poll(
            timeout_ms=self.timeout_ms, max_records=self.batch_size
        )
        docs = []
        for msgs in records.values():
            for msg in msgs:
                doc = msg.value
                if not doc.get("absolute_path"):
                    logger.warning("Skipping metadata message without absolute_path")
                    continue
                docs.append(doc)
        if docs:
            try:
                # Bulk index the batch using DAL
                self.dal.bulk_index(self.index_name, docs)
                logger.info(f"Indexed {len(docs)} metadata documents")
            except Exception as e:
                logger.error(f"Failed to index metadata batch to ES: {e}")
            finally:
                self.metadata_consumer.commit()

    def _process_transcription_messages(self):
        """Process transcription messages"""
        records = self.transcription_consumer.poll(
            timeout_ms=self.timeout_ms, max_records=self.batch_size
        )
        processed = False
        for msgs in records.values():
            for msg in msgs:
                doc = msg.value
                if doc.get("message_type") != "transcription":
                    continue
                doc_id = doc.get("absolute_path")
                content = doc.get("content")
                if not doc_id or not content:
                    logger.warning("Skipping transcription message without absolute_path or content")
                    continue
                try:
                    update_doc = {"content": content}
                    self.dal.index_or_update_doc(self.index_name, doc_id, update_doc)
                    processed = True
                    logger.info(f"Updated document with transcription: {doc_id}")
                except Exception as e:
                    logger.error(f"Failed to update document with transcription: {e}")
        if processed:
            self.transcription_consumer.commit()

    def _restart_consumers(self):
        """Restart both consumers"""
        try:
            self.metadata_consumer.close()
            self.transcription_consumer.close()
        except Exception:
            pass
        self.metadata_consumer = Kafka_Connector.get_consumer(
            self.metadata_topic, self.metadata_group
        )
        self.transcription_consumer = Kafka_Connector.get_consumer(
            self.transcription_topic, self.transcription_group
        )
