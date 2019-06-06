import json
from tempfile import NamedTemporaryFile
from google.cloud import storage


class Storage():

    def __init__(self, root, credential_path=None):
        self.root = root
        self.credential_path = credential_path

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

        self.get_blob(path, check_existance=False)\
            .upload_from_filename(filename=_content_path)

    def download_file(self, path, save_path):
        with open(path, "wb") as f:
            self.get_blob(path).download_to_file(f)

    def download_conent(self, path):
        return self.get_blob(path).download_as_string()

    def exists(self, path):
        return self.get_blob(path, check_existance=False).exists()

    def get_blob(self, path, check_existance=True):
        if self.credential_path is not None:
            client = storage.Client.from_service_account_json(
                        self.credential_path)
        else:
            client = storage.Client()
        blob = client.get_bucket(self.root).blob(path)

        if check_existance and not blob.exists():
            raise Exception("{} does not exist at {}.".format(path, self.root))
        return blob

    def delete(self, path):
        self.get_blob(path).delete()


