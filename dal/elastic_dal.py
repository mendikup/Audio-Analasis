from shared.connectors.elastic_connector import Elastic_Connector
from shared.utils.logger import logger
from elasticsearch import NotFoundError, helpers
import json
from typing import List, Dict


class Elastic_DAL:
    def __init__(self):
        self.es = Elastic_Connector().es

    def index_or_update_doc(self, index_name: str, doc_id: str, doc: Dict) -> None:
        """
        Index new document if id not exists, otherwise update existing document.
        Logs whether it was created or updated.
        """
        try:
            cleaned_doc = self._clean_empty_fields(doc)
            res = self.es.update(
                index=index_name,
                id=doc_id,
                body={"doc": cleaned_doc, "doc_as_upsert": True}
            )

            result = res.get("result")
            if result == "created":
                logger.info(f"Document CREATED in {index_name}: {doc_id}")
            elif result == "updated":
                logger.info(f"Document UPDATED in {index_name}: {doc_id}")
            else:
                logger.warning(f"Unexpected result for {doc_id} in {index_name}: {result}")

        except Exception as e:
            logger.error(f"Failed to index/update doc {doc_id}: {e}")

    def get_by_id(self, index_name: str, doc_id: str) -> Dict:
        try:
            res = self.es.get(index=index_name, id=doc_id)
            return res.get("_source", {})
        except NotFoundError:
            return {}
        except Exception as e:
            logger.error(f"Error retrieving document {doc_id}: {e}")
            return {}

    def bulk_index(self, index_name: str, docs: List[Dict]) -> None:
        actions = []
        for doc in docs:
            cleaned_doc = self._clean_empty_fields(doc)
            doc_id = cleaned_doc.get("absolute_path")
            if not doc_id:
                logger.warning("Skipping document without absolute_path in bulk index")
                continue

            actions.append({
                "_op_type": "index",
                "_index": index_name,
                "_id": doc_id,
                "_source": cleaned_doc,
            })

        if actions:
            try:
                helpers.bulk(self.es, actions)
                logger.info(f"Bulk indexed {len(actions)} documents")
            except Exception as e:
                logger.error(f"Bulk index failed: {e}")
        else:
            logger.warning("No valid documents to bulk index")

    def get_all_data(self, index_name: str, size: int = 1000) -> List[Dict]:
        logger.info(f"Retrieving documents from {index_name}")

        try:
            res = self.es.search(index=index_name, query={"match_all": {}}, size=size)
            docs = [hit["_source"] for hit in res["hits"]["hits"]]
            logger.info(f"Retrieved {len(docs)} documents")
            return docs
        except Exception as e:
            logger.error(f"Failed to retrieve documents: {e}")
            return []

    def _clean_empty_fields(self, doc: Dict) -> Dict:
        cleaned = {}
        for key, value in doc.items():
            if isinstance(value, str):
                if value and value.strip():
                    cleaned[key] = value.strip()
                elif key != "content":
                    cleaned[key] = value
            else:
                cleaned[key] = value
        return cleaned


if __name__ == "__main__":
    dal = Elastic_DAL()
    print(json.dumps(dal.get_all_data("files-metadata"),indent=4))