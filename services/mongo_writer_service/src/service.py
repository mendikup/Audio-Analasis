from shared.connectors.kafka_connector import Kafka_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger
from dal.mongo_dal import Mongo_Dal
import time


class MongoWriter:
    def __init__(self):
        config = load_config()
        self.topic = config["kafka"]["topics"]["raw_metadata"]
        self.group_id = "mongowriter-metadata-group"
        self.batch_size = config["kafka"].get("consumer_batch_size", 50)
        self.timeout_ms = config["kafka"].get("consumer_timeout_ms", 1000)

        # Kafka consumer with manual commit
        self.consumer = Kafka_Connector.get_consumer(self.topic, group_id=self.group_id)

        # Use Mongo DAL for all DB interactions
        self.dal = Mongo_Dal()


    def run(self):
        logger.info("MongoWriter started: waiting for Kafka messages...")
        while True:
            try:
                records = self.consumer.poll(timeout_ms=self.timeout_ms, max_records=self.batch_size)
                if not records:
                    continue
                for msgs in records.values():
                    for msg in msgs:
                        doc = msg.value
                        abs_path = doc.get("absolute_path")
                        # Ensure each doc has the field of absolute path
                        if not abs_path:
                            logger.warning("Skipping message without absolute_path")
                            continue
                        file_id = abs_path
                        filename = doc.get("name")

                        # Store file without overwriting existing content
                        self.dal.store_file(file_id, abs_path, filename, replace=False)  # avoid duplicate uploads
                        # Commit after each file so the same message isn't reprocessed
                        self.consumer.commit()  # manual commit per message

            except Exception as e:
                logger.error(f"Consumer loop error: {e} Restarting consumer in 1s...")
                try:
                    self.consumer.close()
                except Exception:
                    pass
                time.sleep(1)
                self.consumer = Kafka_Connector.get_consumer(self.topic, self.group_id)
                continue
