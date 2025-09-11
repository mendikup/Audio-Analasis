from elasticsearch import Elasticsearch
from shared.utils.logger import logger
from shared.utils.config_loader import load_config


class Elastic_Connector:
    def __init__(self):
        config = load_config()
        url = config["elasticsearch"].get("url", "http://localhost:9200")
        self.es = Elasticsearch(url)
        self.config = config

        try:
            if self.es.ping():
                self._ensure_indexes()
                logger.info("Connected to Elasticsearch successfully")
            else:
                logger.error("Elasticsearch ping failed")
        except Exception as e:
            logger.error(f"Elasticsearch connection error: {e}")

    def _ensure_indexes(self):
        index_name = self.config["elasticsearch"]["indexes"]["files_metadata"]

        files_mapping = {
            "mappings": {
                "properties": {
                    "name": {"type": "keyword"},
                    "absolute_path": {"type": "keyword"},
                    "created": {"type": "date"},
                    "modified": {"type": "date"},
                    "content": {"type": "text"},
                    "hostility_level": {"type": "keyword"},
                    "hostile_word_count": {"type": "integer"},
                    "moderate_word_count": {"type": "integer"},
                    "hostile_words_found": {"type": "keyword"},
                    "moderate_words_found": {"type": "keyword"},
                    "total_word_matches": {"type": "integer"},
                    "text_length": {"type": "integer"},
                    "analysis_timestamp": {"type": "date", "format": "epoch_second"}
                }
            }
        }

        self._create_index_if_not_exists(index_name, files_mapping)

    def _create_index_if_not_exists(self, index_name: str, mapping: dict):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"Index: {index_name} was created successfully")
        else:
            logger.info(f"Index: {index_name} already exists")
