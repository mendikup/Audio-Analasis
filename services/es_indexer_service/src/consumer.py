import time
from dal.elastic_dal import Elastic_DAL
from shared.connectors.kafka_connector import Kafka_Connector
from shared.connectors.elastic_connector import Elastic_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger

class ESIndexer:
    def __init__(self):
        # Subscribe to the same topic produced by the retriever
        cfg = load_config()
        self.topic = cfg["kafka"]["topics"]["raw_metadata"]
        self.consumer = Kafka_Connector.get_consumer(self.topic)
        self.dal =Elastic_DAL()
        self.index_name = "files_metadata"  # Keep a stable index name

    def run(self):
        logger.info("ESIndexer started and waiting for messages...")
        while True:
            try:
                for msg in self.consumer:
                    doc = msg.value
                    # Use absolute_path as the document ID (simple and readable)
                    doc_id = doc.get("absolute_path")
                    if not doc_id:
                        logger.warning("Skipping message without absolute_path")
                        continue
                    try:
                        self.dal.index_doc(index_name=self.index_name, id=doc_id, doc=doc)
                        time.sleep(5)
                        self.dal.get_all_data(self.index_name)
                    except Exception as e:
                        logger.error(f"Failed to index document to ES: {e}")

            except Exception as e:
                logger.error(f"Consumer loop error: {e} Restarting consumer in 1s...")
                try:
                    self.consumer.close()
                except Exception:
                    pass
                time.sleep(1)
                self.consumer = Kafka_Connector.get_consumer(self.topic)
                continue







