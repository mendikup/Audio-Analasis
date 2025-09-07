import os
import yaml

# Always resolve config.yaml relative to this file’s directory
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # ../shared
CONFIG_FILE = os.path.join(BASE_DIR, "config", "config.yaml")

def load_config():
    """
    Load configuration:
    - First try to load from Environment Variables (for deployment).
    - If not found  it goes to config.yaml (for local development).
    """

    #  Try environment variables first
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")
    mongo_collection = os.getenv("MONGO_COLLECTION")




    if all([
        mongo_uri, mongo_db, mongo_collection,
        ]):
        return {
            "mongo": {
                "uri": mongo_uri,
                "db": mongo_db,
                "collection": mongo_collection,
            }

        }

    #  Otherwise, load from YAML file (local development)
    with open(CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)
