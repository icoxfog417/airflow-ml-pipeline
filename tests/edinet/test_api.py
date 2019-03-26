import unittest
import airflow_ml.edinet.api as api


class TestAPI(unittest.TestCase):

    def xtest_metadata(self):
        client = api.MetaDataClient()
        metadata = client.get("2019-01-31")
        self.assertGreater(metadata.count, 0)

    def test_document(self):
        client = api.DocumentClient()
        documents = client.get("2019-01-31")
        self.assertGreater(len(documents), 0)
