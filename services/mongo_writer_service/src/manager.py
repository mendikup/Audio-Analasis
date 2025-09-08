from shared.connectors.kafka_connector import Kafka_Connector
from shared.utils.config_loader import load_config
from shared.utils.logger import logger

from pymongo import MongoClient
from gridfs import GridFS
from pathlib import Path
import time


class MongoWriter:
    def __init__(self):
        config = load_config()
        self.topic = config["kafka"]["topics"]["raw_metadata"]

        # Kafka consumer without timeout
        self.consumer = Kafka_Connector.get_consumer(self.topic)

        # Build Mongo + GridFS connection (later will use the Mongo_Connector tu build)
        mongo_uri = config["mongo"]["uri"]
        db_name = config.get("mongo").get("db")
        bucket_name = config.get("mongo").get("gridfs_bucket", "fs")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.fs = GridFS(self.db, collection=bucket_name)


    def run(self):
        logger.info("MongoWriter started: waiting for Kafka messages...")
        while True:
            try:
                for msg in self.consumer:
                    doc = msg.value
                    abs_path = doc.get("absolute_path")
                    # Ensure each doc has the field of absolute path
                    if not abs_path:
                        logger.warning("Skipping message without absolute_path")
                        continue

                    file_id = abs_path
                    filename = doc.get("name")

                    self._store_file(file_id, abs_path, filename)
            except Exception as e:
                logger.error(f"Consumer loop error: {e} Restarting consumer in 1s...")
                try:
                    self.consumer.close()
                except Exception:
                    pass
                time.sleep(1)
                self.consumer = Kafka_Connector.get_consumer(self.topic)
                continue

    def _store_file(self, file_id: str, abs_path: str, filename: str, replace: bool = True):
        """
        Store a local file in MongoDB GridFS.
        - file_id: unique identifier (we use absolute_path)
        - abs_path: path on the disk
        - replace: if True, delete existing GridFS file with same id
        """
        p = Path(abs_path)

        # Ensure the file exist in the path
        if not (p.exists() and p.is_file()):
            logger.warning(f"File not found: {abs_path}")
            return

        # Ensure the file not already exist, otherwise we delete it to prevent duplicates
        if replace and self.fs.exists({"_id": file_id}):
            self.fs.delete(file_id)

        with p.open("rb") as fh:
            kwargs = {"_id": file_id}
            if filename:
                kwargs["filename"] = filename
            self.fs.put(fh, **kwargs)

        logger.info(f"Stored file in GridFS (id={file_id}, name={filename or p.name})")
