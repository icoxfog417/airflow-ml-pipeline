import os
import json
import shutil
from tempfile import NamedTemporaryFile
from airflow_ml.edinet_flow.storage.storage import Storage


class LocalStorage(Storage):

    def __init__(self, root):
        super().__init__(root, None)

    def upload_file(self, path, content_path="", content=None):
        _content_path = content_path
        if not _content_path:
            if content is not None:
                _content = content
                if isinstance(_content, dict):
                    _content = json.dumps(_content)

                with NamedTemporaryFile(delete=False, mode="w",
                                        encoding="utf-8") as ntf:
                    ntf.write(_content)
                    ntf.flush()
                    _content_path = ntf.name
            else:
                raise Exception("Target content is not spefified")

        target_path = os.path.join(self.root, path)
        shutil.copyfile(_content_path, target_path)

    def download_file(self, path, save_path):
        target_path = os.path.join(self.root, path)
        shutil.copyfile(save_path, target_path)

    def download_conent(self, path, encoding="utf-8"):
        target_path = os.path.join(self.root, path)
        content = ""
        with open(target_path, "r", encoding=encoding) as f:
            content = f.readlines()

        return "\n".join(content)

    def exists(self, path):
        target_path = os.path.join(self.root, path)
        return os.path.exists(target_path)

    def list_objects(self, prefix="", delimiter=""):
        target_path = os.path.join(self.root, prefix)
        for p in os.listdir(target_path):
            yield os.path.join(target_path, p)

    def delete(self, path):
        target_path = os.path.join(self.root, path)
        os.remove(target_path)
