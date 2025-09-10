from shared.connectors.elastic_connector import Elastic_Connector
from shared.utils.logger import logger
import json


class Elastic_DAL:

    def __init__(self):
        self.es = Elastic_Connector().es

    def index_or_update_doc(self, index_name: str, doc_id: str, doc: dict):
        """
        Index new document if id not exists, otherwise update existing document.
        Logs whether it was created or updated.
        """
        try:
            res = self.es.update(
                index=index_name,
                id=doc_id,
                body={
                    "doc": doc,
                    "doc_as_upsert": True
                }
            )

            if res.get("result") == "created":
                logger.info(f"Document CREATED in {index_name}: {doc_id}")
            elif res.get("result") == "updated":
                logger.info(f"Document UPDATED in {index_name}: {doc_id}")
            else:
                logger.warning(f"Document {doc_id} in {index_name} had unexpected result: {res.get('result')}")

        except Exception as e:
            logger.error(f"Failed to index/update doc {doc_id}: {e}")
            raise

    def get_by_id(self,index_name,doc_id):
        """Get document by its ID"""
        res =self.es.get(index=index_name, id=doc_id)
        return res["hits"]["hits"]["_source"]

    def get_all_data(self, index_name):
        """Retrieve all documents from the index."""
        logger.info(f"Retrieving all documents from {index_name}")

        res = self.es.search(index=index_name, query={"match_all": {}}, size=100)
        docs = [hit["_source"] for hit in res["hits"]["hits"]]

        logger.info(f"Retrieved {len(docs)} documents")
        print(json.dumps(docs, indent=4))

