import os
from faster_whisper import WhisperModel
from shared.utils.logger import logger
from elasticsearch import Elasticsearch, NotFoundError
from dal .elastic_dal import Elastic_DAL




class Transcriber:
    def __init__(self):
        """Initialize the Whisper model"""
        self.es = Elastic_DAL().es
        try:
            logger.info("Initializing Whisper model...")
            self.whisper_model = WhisperModel(
                "small",
                device="cpu",
                compute_type="float32"
            )
            logger.info("Whisper model initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Whisper model: {e}")
            raise


    def has_transcription(self, absolute_path: str, index_name: str) -> bool:
        """
        Check if the document already has a transcription
        before we're starting to transcribe
        """
        try:
            res = self.es.get(index=index_name, id=absolute_path)
            src = res.get("_source", {})
            return bool(src.get("content") != " ")
        except NotFoundError:
            return False
        except Exception as e:
            logger.error(f"Failed to check transcription for {absolute_path}: {e}")
            # return false to retry transcription fot this audio-file
            return False

    def transcribe_audio_file(self, file_path: str) -> str:
        """
        Transcribe an audio file using faster-whisper

        Args: file_path (str): Absolute path to the audio file

        Returns: str: Transcribed text, or None if transcription failed
        """
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                logger.error(f"Audio file not found: {file_path}")
                return None

            logger.info(f"Starting transcription for: {file_path}")
            # Transcribe the audio file
            segments, info = self.whisper_model.transcribe(file_path)

            # Combine all segments into one text
            transcription = " ".join([segment.text.strip() for segment in segments])

            logger.info(f"Transcription completed. Language: {info.language}, Duration: {info.duration:.2f}s")
            return transcription.strip()

        except Exception as e:
            logger.error(f"Error transcribing audio file {file_path}: {e}")
            return None


