from pymongo import MongoClient
from pymongo.errors import PyMongoError
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
