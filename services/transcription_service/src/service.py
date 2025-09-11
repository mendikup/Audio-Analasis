import time
from services.transcription_service.src.transcriber import Transcriber
from shared.connectors.kafka_connector import Kafka_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger


class TranscriptionService:
    def __init__(self):
        config = load_config()
        self.input_topic = config["kafka"]["topics"]["raw_metadata"]
        self.output_topic = config["kafka"]["topics"]["transcribed_content"]
        self.group_id = "transcription-service-group"
        self.batch_size = config["kafka"].get("consumer_batch_size", 50)
        self.timeout_ms = config["kafka"].get("consumer_timeout_ms", 1000)

        self.consumer = Kafka_Connector.get_consumer(
            self.input_topic, group_id=self.group_id
        )
        self.producer = Kafka_Connector.get_producer()
        self.dal = Transcriber()
        # Index name from config to check for existing transcriptions
        self.index_name = config["elasticsearch"]["indexes"]["files_metadata"]

    def run(self):
        logger.info("TranscriptionService started and waiting for messages...")
        while True:
            try:
                records = self.consumer.poll(
                    timeout_ms=self.timeout_ms, max_records=self.batch_size
                )
                if not records:
                    continue
                for msgs in records.values():
                    for msg in msgs:
                        doc = msg.value
                        absolute_path = doc.get("absolute_path")
                        if not absolute_path:
                            logger.warning("Skipping message without absolute_path")
                            continue
                        # Check if it's an audio file
                        if not self._is_audio_file(absolute_path):
                            logger.debug(f"Skipping non-audio file: {absolute_path}")
                            continue
                        # Skip already transcribed files
                        if self.dal.has_transcription(absolute_path, self.index_name):
                            logger.info(f"Skipping already-transcribed file: {absolute_path}")
                            continue
                        try:
                            transcription = self.dal.transcribe_audio_file(absolute_path)
                            if transcription:
                                transcription_doc = {
                                    "absolute_path": absolute_path,
                                    "content": transcription,
                                    "message_type": "transcription",
                                }
                                self.producer.send(self.output_topic, transcription_doc)
                                logger.info(
                                    f"Transcription completed and sent for: {absolute_path}"
                                )
                        except Exception as e:
                            logger.error(
                                f"Failed to process transcription for {absolute_path}: {e}"
                            )
                self.consumer.commit()
            except Exception as e:
                logger.error(
                    f"Consumer loop error: {e}. Restarting consumer in 1s..."
                )
                try:
                    self.consumer.close()
                except Exception:
                    pass
                time.sleep(1)
                self.consumer = Kafka_Connector.get_consumer(
                    self.input_topic, self.group_id
                )
                continue

    def _is_audio_file(self, file_path: str) -> bool:
        """Check if the file is an audio file based on extension"""
        audio_extensions = {".mp3", ".wav", ".m4a", ".flac", ".aac", ".ogg", ".wma"}
        return any(file_path.lower().endswith(ext) for ext in audio_extensions)
