
import time
import base64
from typing import List, Dict, Any
from dal.elastic_dal import Elastic_DAL
from shared.connectors.kafka_connector import Kafka_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger
from services.hostility_detection_service.src.analyzer import Analyzer


class HostilityDetector:
    def __init__(self):
        # Load configuration
        cfg = load_config()
        self.input_topic = cfg["kafka"]["topics"]["transcribed_content"]
        self.output_topic = cfg["kafka"]["topics"]["hostility_results"]
        self.consumer = Kafka_Connector.get_consumer(self.input_topic,
                        group_id="hostility-detector-transcription-group")

        self.producer = Kafka_Connector.get_producer()
        self.dal = Elastic_DAL()
        self.index_name = cfg["elasticsearch"]["indexes"]["hostility_results"]

        # Set threshold for flagging messages
        self.DANGER_THRESHOLD = 15.0

        self.analyzer = Analyzer()

        # Load encrypted word lists
        self._load_word_lists()

    def _load_word_lists(self):
        """Load and decode encrypted word lists from base64"""

        # More hostile list for Israel (double weight)
        hostile_encoded = "R2Vub2NpZGUSV2FyIENyaW1lcyxBcGFydGhlaWQsTWFzc2FjcmUsTmFrYmEsRGlzcGxhY2VtZW50LEh1bWFuaXRhcmlhbiBDcmlzaXMsQmxvY2thZGUST2NjdXBhdGlvbixSZWZ1Z2VlcyxJQ0MsQkRT"

        # Less hostile list for Israel (regular weight)
        moderate_encoded = "RnJlZWRvbSBGbG90aWxsYSxSZXNpc3RhbmNlLExpYmVyYXRpb24sRnJlZSBQYWxlc3RpbmUsR2F6YSxDZWFzZWZpcmUsUHJvdGVzdCxVTlJXQQ=="

        try:
            # Decode base64 to regular text
            hostile_text = base64.b64decode(hostile_encoded).decode('utf-8')
            moderate_text = base64.b64decode(moderate_encoded).decode('utf-8')

            # Split into word lists (words separated by comma )
            self.hostile_words = [word.strip().lower() for word in hostile_text.split(',') if word.strip()]
            self.moderate_words = [word.strip().lower() for word in moderate_text.split(',') if word.strip()]

            # Problematic word pairs (what I found )
            self.problem_pairs = [
                ("free", "palestine"),
                ("israeli", "occupation"),
                ("war", "crimes"),
                ("human", "rights"),
                ("gaza", "massacre")
            ]

            logger.info(
                f"Successfully loaded: {len(self.hostile_words)} hostile words, {len(self.moderate_words)} moderate words")

        except Exception as e:
            logger.error(f"Error decoding word lists: {e}")
            # Default fallback - empty lists
            self.hostile_words = []
            self.moderate_words = []
            self.problem_pairs = []

    def run(self):
        """Start the service  read from topic which sent by the transcriber  and analyze content"""
        logger.info("Hostility Detector service started and waiting for messages...")

        while True:
            try:
                # Read messages from Kafka
                for msg in self.consumer:
                    doc = msg.value
                    text_content = doc.get("content", "")
                    doc_id = doc.get("absolute_path")

                    # Ensure there's content to analyze
                    if not doc_id or not text_content:
                        logger.warning("Skipping message without required fields")
                        continue

                    # Analyze content for hostility
                    analysis_result = self.analyzer._analyze_content(text_content, doc_id,
                                                                     self.problem_pairs,
                                                                     self.DANGER_THRESHOLD,
                                                                     self.moderate_words
                                                                     ,self.hostile_words)

                    # Send results
                    self._send_to_kafka(analysis_result)
                    self._save_to_elasticsearch(analysis_result)
                    # Commit offset so this message won't be read again
                    self.consumer.commit()  # manual commit per message

            except Exception as e:
                logger.error(f"Consumer loop error: {e} - restarting...")
                # Retry to create the consumer again after error
                try:
                    self.consumer.close()
                except:
                    pass
                time.sleep(1)
                self.consumer = Kafka_Connector.get_consumer(self.input_topic,
                                group_id="hostility-detector-transcription-group")


    def _send_to_kafka(self, result: Dict[str, Any]):
        """Send result to Kafka"""
        try:
            self.producer.send(self.output_topic, result)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")

    def _save_to_elasticsearch(self, result: Dict[str, Any]):
        """Save result to Elasticsearch"""
        try:
            doc_id = result["document_id"]
            self.dal.index_or_update_doc(
                index_name=self.index_name,
                doc_id=doc_id,
                doc=result
            )
        except Exception as e:
            logger.error(f"Error saving to Elasticsearch: {e}")
