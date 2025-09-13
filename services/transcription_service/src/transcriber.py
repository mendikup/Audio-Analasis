import os

from faster_whisper import WhisperModel
from shared.utils.logger import logger
from elasticsearch import NotFoundError
from dal.elastic_dal import Elastic_DAL


class Transcriber:
    def __init__(self):
        """Initialize the Whisper model"""
        self.dal = Elastic_DAL()
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
        Check if the document already has a meaningful transcription
        before we're starting to transcribe
        """
        try:
            doc = self.dal.get_by_id(index_name, absolute_path)
            content = doc.get("content", "")

            # בדיקה חזקה יותר - התוכן חייב להיות קיים ולא ריק ולא רק רווחים
            if not content or not content.strip():
                logger.debug(f"No meaningful content found for {absolute_path}")
                return False

            # בדיקה נוספת - אם יש פחות מ-5 תווים, כנראה שזה לא תמלול אמיתי
            if len(content.strip()) < 5:
                logger.debug(f"Content too short for {absolute_path}: '{content.strip()}'")
                return False

            logger.debug(f"Found existing transcription for {absolute_path} (length: {len(content.strip())})")
            return True

        except NotFoundError:
            logger.debug(f"Document not found in ES: {absolute_path}")
            return False
        except Exception as e:
            logger.error(f"Failed to check transcription for {absolute_path}: {e}")
            # return false to retry transcription for this audio-file
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

            # וודא שהתמלול לא ריק לאחר העיבוד
            if not transcription or not transcription.strip():
                logger.warning(f"Empty transcription result for: {file_path}")
                return None

            logger.info(
                f"Transcription completed. Language: {info.language}, Duration: {info.duration:.2f}s, Length: {len(transcription.strip())} chars")
            return transcription.strip()

        except Exception as e:
            logger.error(f"Error transcribing audio file {file_path}: {e}")
            return None