from elasticsearch import Elasticsearch
from shared.utils.logger import logger
from shared.utils.config_loader import load_config

class Elastic_Connector:

    def __init__(self):
        config = load_config()
        url = config.get("url", "http://localhost:9200")
        self.es = Elasticsearch(url)


        try:
            if self.es.ping():
                self._ensure_index()
                logger.info("Connected to Elasticsearch successfully")
            else:
                logger.error("Elasticsearch ping failed")
        except Exception as e:
            logger.error(f"Elasticsearch connection error: {e}")


    def _ensure_index(self, index_name: str):
        """
        create a new index and mapping if mot exist
        """
        mapping = {
            "mappings": {
                "properties": {
                    "id" : {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "absolute_path": {"type": "keyword"},
                    # "size_bytes": {"type": "long"},
                    "created": {"type": "date"},
                    "modified": {"type": "date"}

                }
            }
        }
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, mappings=mapping)
            logger.info(f"Index:  {index_name} was created successfully")


