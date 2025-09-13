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
                    # מטאדטה של קבצים
                    "name": {"type": "keyword"},
                    "absolute_path": {"type": "keyword"},
                    "created": {"type": "date"},
                    "modified": {"type": "date"},

                    # תוכן התמלול - TEXT עבור חיפושים מתקדמים
                    "content": {
                        "type": "text",
                        "analyzer": "standard",
                        "search_analyzer": "standard",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            },
                            "length": {
                                "type": "token_count",
                                "analyzer": "standard"
                            }
                        }
                    },

                    # שדות ניתוח עוינות (מה-hostility detection service)
                    "bds_percent": {"type": "float"},
                    "is_bds": {"type": "boolean"},
                    "bds_threat_level": {"type": "keyword"},
                    "document_id": {"type": "keyword"},

                    # שדות נוספים לניתוח (אם נדרש בעתיד)
                    "hostility_level": {"type": "keyword"},
                    "hostile_word_count": {"type": "integer"},
                    "moderate_word_count": {"type": "integer"},
                    "hostile_words_found": {"type": "keyword"},
                    "moderate_words_found": {"type": "keyword"},
                    "total_word_matches": {"type": "integer"},
                    "text_length": {"type": "integer"},
                    "analysis_timestamp": {"type": "date", "format": "epoch_second"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "content_analyzer": {
                            "type": "standard",
                            "stopwords": "_english_"
                        }
                    }
                }
            }
        }

        self._create_index_if_not_exists(index_name, files_mapping)

    def _create_index_if_not_exists(self, index_name: str, mapping: dict):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"Index: {index_name} was created successfully with enhanced text mapping")
        else:
            logger.info(f"Index: {index_name} already exists")

            # אפשרות לעדכן את המיפוי אם צריך (זהירות - יכול להיות מסוכן)
            # self._update_mapping_if_needed(index_name, mapping["mappings"])

    def _update_mapping_if_needed(self, index_name: str, mapping: dict):
        """
        עדכון זהיר של מיפוי קיים (רק שדות חדשים)
        זהירות: לא ניתן לשנות שדות קיימים!
        """
        try:
            self.es.indices.put_mapping(index=index_name, body=mapping)
            logger.info(f"Updated mapping for index: {index_name}")
        except Exception as e:
            logger.warning(f"Could not update mapping for {index_name}: {e}")