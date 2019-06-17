class Storage():

    def __init__(self, root, credential_path=None):
        self.root = root
        self.credential_path = credential_path

    def upload_file(self, path, content_path="", content=None):
        raise NotImplementedError("Storage should implements upload_file method.")

    def download_file(self, path, save_path):
        raise NotImplementedError("Storage should implements download_file.")

    def download_conent(self, path, encoding="utf-8"):
        raise NotImplementedError("Storage should implements download_content.")

    def exists(self, path):
        raise NotImplementedError("Storage should implements exists.")

    def list_objects(self, prefix="", delimiter=""):
        raise NotImplementedError("Storage should implements list_objects.")

    def delete(self, path):
        raise NotImplementedError("Storage should implements delete.")

    @classmethod
    def gcs(cls, root, credential_path=None):
        from airflow_ml.edinet_flow.storage.gcs_storage import GCSStorage
        return GCSStorage(root, credential_path)

    @classmethod
    def s3(cls, root, credential_path=None):
        from airflow_ml.edinet_flow.storage.s3_storage import S3Storage
        return S3Storage(root, credential_path)
