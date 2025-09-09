from shared.connectors.elastic_connector import Elastic_Connector
from shared.utils.logger import logger
import json


class Elastic_DAL:
    def __init__(self):
        self.es = Elastic_Connector().es

    def index_doc(self, index_name, doc_id, doc):
        self.es.index(index=index_name, id=doc_id, document=doc)
        logger.info(f"Indexed to ES: {doc_id}")

    def update_doc(self, index_name, doc_id, doc):
        """Update an existing document"""
        self.es.update(index=index_name, id=doc_id, body={"doc": doc})
        logger.info(f"Updated in ES: {doc_id}")

    def get_by_id(self,index_name,doc_id):
        """Get document by its ID"""
        res =self.es.get(index=index_name, id=doc_id)
        return res["hits"]["hits"]["_source"]

    def get_all_data(self, index_name):
        """Retrieve all documents from the index."""
        logger.info(f"Retrieving all documents from {index_name}")

        res = self.es.search(index=index_name, query={"match_all": {}}, size=1000)
        docs = [hit["_source"] for hit in res["hits"]["hits"]]

        logger.info(f"Retrieved {len(docs)} documents")
        print(json.dumps(docs, indent=4))