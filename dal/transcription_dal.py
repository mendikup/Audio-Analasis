import os
from faster_whisper import WhisperModel
from shared.utils.logger import logger


class Transcription_DAL:
    def __init__(self):
        """Initialize the Whisper model"""
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

if __name__ =="__main__":
    t =Transcription_DAL()
    print(t.transcribe_audio_file("C:\podcasts\download (2).wav"))