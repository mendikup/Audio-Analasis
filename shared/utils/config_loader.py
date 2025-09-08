import os
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # ../shared
CONFIG_FILE = os.path.join(BASE_DIR, "config", "config.yaml")


def load_config():
    """Load configuration from config.yaml with optional overrides from
    environment variables
    """
    # Load defaults from YAML file
    with open(CONFIG_FILE, "r") as f:
        config = yaml.safe_load(f)

    # Make sure all varibales exist
    if "mongo" not in config:
        config["mongo"] = {}
    if "kafka" not in config:
        config["kafka"] = {}
    if "files" not in config:
        config["files"] = {}

    #  Mongo overrides
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")
    mongo_collection = os.getenv("MONGO_COLLECTION")

    if mongo_uri:
        config["mongo"]["uri"] = mongo_uri
    if mongo_db:
        config["mongo"]["db"] = mongo_db
    if mongo_collection:
        config["mongo"]["collection"] = mongo_collection

    # Kafka overrides
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_raw = os.getenv("KAFKA_TOPIC_RAW_METADATA")

    if kafka_bootstrap:
        config["kafka"]["bootstrap_servers"] = kafka_bootstrap
    if kafka_topic_raw:
        if "topics" not in config["kafka"]:
            config["kafka"]["topics"] = {}
        config["kafka"]["topics"]["raw_metadata"] = kafka_topic_raw

    # Files path overrides
    files_path = os.getenv("FILES_PATH")
    if files_path:
        config["files"]["path"] = files_path

    return config