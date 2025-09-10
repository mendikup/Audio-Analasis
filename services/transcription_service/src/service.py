import time
from services.transcription_service.src.transcriber import TranscriptionDal
from shared.connectors.kafka_connector import Kafka_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger


class TranscriptionService:
    def __init__(self):
        config = load_config()
        self.input_topic = config["kafka"]["topics"]["raw_metadata"]
        self.output_topic = config["kafka"]["topics"]["transcribed_content"]

        self.consumer = Kafka_Connector.get_consumer(
            self.input_topic,
            group_id="transcription-service-group"
        )
        self.producer = Kafka_Connector.get_producer()
        self.dal = TranscriptionDal()
        self.index_name = "files_metadata"


    def run(self):
        logger.info("TranscriptionService started and waiting for messages...")
        while True:
            try:
                for msg in self.consumer:
                    doc = msg.value
                    absolute_path = doc.get("absolute_path")

                    if not absolute_path:
                        logger.warning("Skipping message without absolute_path")
                        continue

                    # Check if it's an audio file
                    if not self._is_audio_file(absolute_path):
                        logger.debug(f"Skipping non-audio file: {absolute_path}")
                        continue

                    # Check if this document has a transcription
                    if self.dal.has_transcription(absolute_path,self.index_name):
                        logger.info(f"Skipping already-transcribed file: {absolute_path}")
                        continue

                    try:
                        # Transcribe the audio file
                        transcription = self.dal.transcribe_audio_file(absolute_path)

                        if transcription:
                            # Send transcription to Kafka for ES indexer
                            transcription_doc = {
                                "absolute_path": absolute_path,
                                "content": transcription,
                                "message_type": "transcription"
                            }

                            self.producer.send(self.output_topic, transcription_doc)
                            logger.info(f"Transcription completed and sent for: {absolute_path}")

                    except Exception as e:
                        logger.error(f"Failed to process transcription for {absolute_path}: {e}")

            except Exception as e:
                logger.error(f"Consumer loop error: {e}. Restarting consumer in 1s...")
                try:
                    self.consumer.close()
                except Exception:
                    pass
                time.sleep(1)
                self.consumer = Kafka_Connector.get_consumer(self.input_topic,"transcription-service-group")
                continue

    def _is_audio_file(self, file_path: str) -> bool:
        """Check if the file is an audio file based on extension"""
        audio_extensions = {'.mp3', '.wav', '.m4a', '.flac', '.aac', '.ogg', '.wma'}
        return any(file_path.lower().endswith(ext) for ext in audio_extensions)