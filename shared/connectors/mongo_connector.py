from pymongo import MongoClient
from pymongo.errors import PyMongoError
from gridfs import GridFS
from shared.utils.config_loader import load_config
from shared.utils.logger import logger

class Mongo_Connector:
    @staticmethod
    def get_mongo_collection():
        # Read Mongo config (env overrides are handled in load_config)
        config = load_config().get("mongo")
        uri = config.get("uri")
        db = config.get("db")
        collection = config.get("collection")
        if not all([uri, db, collection]):
            raise ValueError("Mongo config is missing one of: uri/db/collection")
        try:
            client = MongoClient(uri)
            # Fail fast if server is unreachable
            client.admin.command("ping")
            return client[db][collection]
        except PyMongoError as e:
            logger.error(f"Mongo connection failed: {e}")
            raise

    @staticmethod
    def get_gridfs_bucket():
        """Return a GridFS bucket based on configuration."""
        config = load_config().get("mongo")
        uri = config.get("uri")
        db = config.get("db")
        bucket = config.get("gridfs_bucket", "fs")
        if not all([uri, db]):
            raise ValueError("Mongo config is missing one of: uri/db")
        try:
            client = MongoClient(uri)
            client.admin.command("ping")
            return GridFS(client[db], collection=bucket)
        except PyMongoError as e:
            logger.error(f"Mongo GridFS connection failed: {e}")
            raise
