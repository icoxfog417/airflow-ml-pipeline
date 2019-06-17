import json
import csv
from tempfile import NamedTemporaryFile
import boto3
from airflow_ml.edinet_flow.storage.storage import Storage


class S3Storage(Storage):

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

        client = self._get_client()
        client.upload_file(str(_content_path), self.root, path)

    def download_file(self, path, save_path):
        client = self._get_client()
        client.download_file(self.root, path, save_path)

    def download_conent(self, path, encoding="utf-8"):
        client = self._get_client()
        object = client.get_object(Bucket=self.root, Key=path)
        content = object["Body"].read().decode(encoding)
        return content

    def exists(self, path):
        objects = self.list_objects(prefix=path)
        return True if path in objects else False

    def list_objects(self, prefix="", delimiter=""):
        objects = self._get_client().list_objects_v2(
                            Bucket=self.root, Prefix=prefix,
                            Delimiter=delimiter)

        iterator = [] if "Contents" not in objects else objects["Contents"]
        for o in iterator:
            yield o["Key"]

    def _get_client(self):
        if self.credential_path is not None:
            key_id = ""
            secret_key = ""
            with open(self.credential_path, "r") as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    key_id, secret_key = [e.strip() for e in row]
                    break

            client = boto3.client(
                "s3",
                aws_access_key_id=key_id,
                aws_secret_access_key=secret_key
            )

        else:
            client = boto3.client("s3")
        return client

    def delete(self, path):
        self._get_client().delete_object(
            Bucket=self.root, Key=path)
