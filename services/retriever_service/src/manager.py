from dal.files_dal import Files_Loader
from shared.connectors.kafka_connector import Kafka_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger
import json

class Manager:

    def __init__(self):
        self.config = load_config()
        self.topic =self.config["kafka"]["topics"]["raw_metadata"]
        self.files_loder = Files_Loader ()
        self.producer = Kafka_Connector.get_producer()


    def run_all(self):
        logger.info("=========PIPELINE STARTING==========")
        docs =self.load_files_metadata()
        self.send_to_kafka(docs)


    def load_files_metadata(self):
        docs = self.files_loder.get_files_meta_data()
        return  docs

    def send_to_kafka(self,docs):
            for doc in docs:
                # Send the message
                future = self.producer.send(self.topic, doc)
                # Wait for the send to complete (so we catch errors immediately)
                future.get(timeout=10000)
                # Ensure all messages are sent before closing
                self.producer.flush()











