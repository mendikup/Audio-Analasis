
from shared.connectors.elastic_connector import Elastic_Connector
from shared.utils.logger import logger
import json

class Elastic_DAL:

    def __init__(self):
        self.es = Elastic_Connector().es

    def index_doc(self ,index_name,doc_id,doc):

        self.es.index(index=index_name,id= doc_id ,document=doc)
        logger.info(f"Indexed to ES: {doc_id}")

    def get_by_id(self,index_name,doc_id):
        res =self.es.get(index=index_name, id=doc_id)
        print(res)

    def get_all_data(self,index_name):
        """Retrieve all documents from the index."""
        logger.info(f"Retrieving all documents from {index_name} index")

        res = self.es.search(index=index_name, query={"match_all": {}}, size=10000)
        docs = [hit["_source"] for hit in res["hits"]["hits"]]

        logger.info(f"Retrieved {len(docs)} documents")

        print(json.dumps(docs ,indent= 4))

