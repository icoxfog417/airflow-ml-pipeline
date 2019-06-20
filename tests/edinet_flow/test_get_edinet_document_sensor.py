import os
import io
import logging
import unittest
import pytest
from airflow import DAG, configuration
from airflow.utils import timezone
from airflow_ml.edinet_flow.workflow import GetEDINETDocumentSensor, EDINETMixin


DEFAULT_DATE = timezone.datetime(2019, 6, 4)


class TestGetEDINETDocumentSensor(unittest.TestCase):

    def setUp(self):
        super().setUp()
        configuration.load_test_config()
        self.dag = DAG(
            "get_edinet_dl_dag",
            default_args={
                "owner": "airflow_ml",
                'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)

    @classmethod
    def tearDownClass(cls):
        edinet = EDINETMixin()
        path = edinet.list_path_at(DEFAULT_DATE)
        if edinet.storage.exists(path):
            edinet.storage.delete(path)

        path = edinet.document_path_at(DEFAULT_DATE)
        iterator = edinet.storage.list_objects(path)
        for p in iterator:
            edinet.storage.delete(p)

    @pytest.fixture(autouse=True)
    def inject_logger(self, caplog):
        self._caplog = caplog

    def test_execute_without_file(self):
        task = GetEDINETDocumentSensor(
                task_id="get_edinet_d", dag=self.dag,
                poke_interval=2, max_retrieve=1)

        log_stream = io.StringIO()
        stream_handler = logging.StreamHandler(log_stream)
        task.logger.addHandler(stream_handler)

        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                 ignore_ti_state=True)

        log = log_stream.getvalue()
        log_stream.close()
        correct_log = "Document @ 2019/06/04 does not found"
        self.assertTrue(correct_log in log)

    def test_execute_with_file(self):
        test_file_name = f"data/{DEFAULT_DATE.strftime('%Y-%m-%d')}.json"
        num_file = 2
        task = GetEDINETDocumentSensor(
                max_retrieve=num_file, document_types=("120",),
                task_id="get_edinet_d", dag=self.dag, poke_interval=2)

        test_file = os.path.join(os.path.dirname(__file__), test_file_name)
        task.storage.upload_file(task.list_path_at(DEFAULT_DATE),
                                 content_path=test_file)

        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                 ignore_ti_state=True)

        documents = task.get_documents_at(DEFAULT_DATE)
        target = [d for d in documents.list if d.document_id == "S100FTFN"][0]
        result = task.get_file_path(target)
        self.assertEqual(result["xbrl"], "documents/2019-06-04/S100FTFN.xbrl")
        self.assertEqual(result["pdf"], "documents/2019-06-04/S100FTFN.pdf")

        iterator = task.storage.list_objects(task.document_path_at(DEFAULT_DATE))
        count = 0
        for i, b in enumerate(iterator):
            self.assertTrue("S100FTFN" in b or "S100FVMU" in b)
            count += 1
            task.storage.delete(b)

        self.assertEqual(count, num_file * 2)  # xbrl and pdf
        test_file_path = task.list_path_at(DEFAULT_DATE)
        task.storage.delete(test_file_path)
