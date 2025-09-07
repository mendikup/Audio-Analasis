from elasticsearch import Elasticsearch, helpers
from shared.utils.logger import logger


class Elastic_Connector:


    def __init__(self):
        """Initialize connection to Elasticsearch server."""
        self.es = Elasticsearch('http://localhost:9200')
        if self.es.ping():

            logger.info("Connected to Elasticsearch successfully")
        else:
            logger.error("Failed to connect to Elasticsearch")
