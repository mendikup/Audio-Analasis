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
        self.consumer = Kafka_Connector.get_consumer(self.TOPIC)


    def run_all(self):
        logger.info("=========PIPLINE STARTING==========")
        self._load_files_metadata()
        self._send_to_kafka()
        self._get_topic_masseges()


    def _load_files_metadata(self):
        self.files_loder.get_files_meta_data()
        self.files_loder.write_to_json_file()

    def _send_to_kafka(self):
        with open("data/files_metadata.json", "r", encoding="utf-8") as f:
            docs = json.load(f)
            for doc in docs:
                # Send the message
                self._produce_an_event(doc)


    def _produce_an_event(self,doc):
        """
        publish a document to a specific topic to the kafka server
        :param doc: the event to upload
        """
        future= self.producer.send(self.topic,doc)

        # Wait for the send to complete (so we catch errors immediately)
        future.get(timeout=10000)

        # Ensure all messages are sent before closing
        self.producer.flush()











