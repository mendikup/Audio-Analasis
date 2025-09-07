from pathlib import Path
import json
import time

class Files_Loader:

    def __init__(self):
     self.PATH = Path("C:/podcasts")
     self.data = None


    def get_files_meta_data(self):

        data = []

        for file in self.PATH.iterdir():
            if file.is_file():
                stats = file.stat()
                file_info = {
                    "name": file.name,
                    "absolute_path": str(file.resolve()),
                    # "size_bytes": stats.st_size,
                    "created": time.ctime(stats.st_ctime),
                    "modified": time.ctime(stats.st_mtime),
                }
                data.append(file_info)
            self.data = data


    def write_to_json_file(self):
        with open("data/files_metadata.json", "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)


