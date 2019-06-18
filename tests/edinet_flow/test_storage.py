import os
import unittest
import json
from airflow_ml.edinet_flow.storage import Storage


class TestStorage(unittest.TestCase):

    def test_upload_gcs(self):
        item = {
            "value": 1,
            "key": "hogehoge"
        }
        file_name = "test_body.join"
        credential_path = os.path.join(os.path.dirname(__file__),
                                       "../../credential.json")

        storage = Storage.gcs("edinet-data-store", credential_path)
        storage.upload_file(file_name, content=json.dumps(item))

        uploaded = storage.download_conent(file_name)
        self.assertTrue(uploaded)

        storage.delete(file_name)
        self.assertFalse(storage.exists(file_name))

    def test_upload_s3(self):
        item = {
            "value": 1,
            "key": "hogehoge"
        }
        storage = Storage.s3("chakki.edi-test.jp")

        file_name = "lists/test_body.join"
        storage.upload_file(file_name, content=json.dumps(item))

        self.assertTrue(storage.exists(file_name))

        uploaded = storage.download_conent(file_name)
        print(uploaded)
        self.assertTrue(json.loads(uploaded))

        storage.delete(file_name)
        self.assertFalse(storage.exists(file_name))
