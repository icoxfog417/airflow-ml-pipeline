import os
import json
from datetime import timedelta
from django.test import TestCase
from airflow import DAG, configuration
from airflow.utils import timezone
from airflow_ml.edinet_flow.workflow import EDINETMixin
from airflow_ml.edinet_flow.workflow import GetEDINETDocumentListOperator
from airflow_ml.edinet_flow.workflow import GetEDINETDocumentSensor
from airflow_ml.edinet_flow.workflow import RegisterDocumentOperator
from eagle.models.masters import EDINETDocument


DEFAULT_DATE = timezone.datetime(2019, 6, 7)
# https://disclosure.edinet-fsa.go.jp/api/v1/documents.json?type=2&date=2019-06-07


class TestRegisterDocumentOperator(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        configuration.load_test_config()
        dag_id = "register_document_prepare_dag"
        cls.prepare_dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)

        get_list = GetEDINETDocumentListOperator(
                task_id="get_document_list", dag=cls.prepare_dag)
        get_document = GetEDINETDocumentSensor(
                max_retrieve=3, document_types=("120",),
                task_id="get_document", dag=cls.prepare_dag, poke_interval=2)
        cls.prepare_dag.clear()
        get_list.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        get_document.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

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
        cls.prepare_dag.clear()

    def setUp(self):
        super().setUp()
        configuration.load_test_config()
        self.dag = DAG(
            "register_document_dag",
            default_args={
                "owner": "airflow_ml",
                'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)

    def test_register_document(self):
        task = RegisterDocumentOperator(
                max_retrieve=3, document_types=("120",),
                task_id="register_document", dag=self.dag)

        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                 ignore_ti_state=True)

        documents = task.storage.list_objects(
                        task.document_path_at(DEFAULT_DATE))
        documents = sorted(documents)
        documents = [d for d in documents if d.endswith(".pdf")]
        range = [DEFAULT_DATE, DEFAULT_DATE + timedelta(days=1)]
        documents_in_db = EDINETDocument.objects\
                            .filter(submitted_date__range=range)\
                            .order_by("edinet_document_id")

        self.assertEqual(len(documents), len(documents_in_db))
        for d, dd in zip(documents, documents_in_db):
            name, _ = os.path.splitext(os.path.basename(d))
            self.assertEqual(name, dd.edinet_document_id)
