from pathlib import Path
from shared.connectors.mongo_connector import Mongo_Connector
from shared.utils.logger import logger


class Mongo_Dal:
    """A class that manages all queries with the DB"""

    def __init__(self):
        # Initialize collection and GridFS bucket via shared connector
        self.coll = Mongo_Connector.get_mongo_collection()
        self.fs = Mongo_Connector.get_gridfs_bucket()

    def insert_doc(self, doc: dict):
        return self.coll.insert_one(doc)

    def get_all_docs(self):
        return list(self.coll.find().limit(20))

    def store_file(self, file_id: str, abs_path: str, filename: str, replace: bool = True):
        """Store a local file in GridFS via the DAL."""
        p = Path(abs_path)
        if not (p.exists() and p.is_file()):
            logger.warning(f"File not found: {abs_path}")
            return
        if replace and self.fs.exists({"_id": file_id}):
            self.fs.delete(file_id)
        with p.open("rb") as fh:
            kwargs = {"_id": file_id}
            if filename:
                kwargs["filename"] = filename
            self.fs.put(fh, **kwargs)
        logger.info(f"Stored file in GridFS (id={file_id}, name={filename or p.name})")
