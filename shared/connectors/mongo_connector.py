

from pymongo import MongoClient
from shared.utils.config_loader import load_config

class Mongo_Connector:
    """a class that responsible to create and return connections to mongo """

    @staticmethod
    def get_mongo_collection():
        config = load_config()
        client = MongoClient(config["mongo"]["uri"])
        return client[config["mongo"]["db"]][config["mongo"]["collection"]]
