import json
from tempfile import NamedTemporaryFile
from google.cloud import bigquery


class Table():

    def __init__(self, credential_path=None):
        self.credential_path = credential_path

    @property
    def client(self):
        if self.credential_path is not None:
            client = bigquery.Client.from_service_account_json(
                        self.credential_path)
        else:
            client = bigquery.Client()
        return client

    def query(self, query):
        query_job = self.client.query(query)
        rows = query_job.result()
        return rows
