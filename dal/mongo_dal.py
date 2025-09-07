
import json
from shared.connectors.mongo_connector import Mongo_Connector

class Mongo_Dal:
    """
    A class that manages all queries with the DB
    """

    def __init__(self):
        self.coll = Mongo_Connector.get_mongo_collection()

    def insert_doc(self,doc: dict):
        col = self.coll
        return col.insert_one(doc)

    def get_all_docs(self):
        col = self.coll
        return list(col.find().limit(20))

