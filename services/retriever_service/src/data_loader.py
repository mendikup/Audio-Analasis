from pathlib import Path
from typing import List, Dict
from datetime import datetime
from shared.utils.logger import logger
from shared.utils.config_loader import load_config
import os

class Files_Loader:
    def __init__(self):
        config = load_config()
        # sets base path from env or confi
        path = os.getenv("FILES_PATH") or config.get("files").get("path")
        self.path = Path(path)

    def get_files_meta_data(self) -> List[Dict]:
        # Scan directory
        if not self.path.exists():
            logger.error(f"Path not found: {self.path}")


        records: List[Dict] = []
        for file in self.path.iterdir():
            if file.is_file():
                stats = file.stat()
                records.append({
                    "name": file.name,
                    "absolute_path": str(file.resolve()),
                    "created": datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    "modified": datetime.fromtimestamp(stats.st_mtime).isoformat(),
                    "content": ""
                })
                logger.info(f"Loaded metadata for {file.name}")
        return records
