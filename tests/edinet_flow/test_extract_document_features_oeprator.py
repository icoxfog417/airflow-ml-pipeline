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
from airflow_ml.edinet_flow.workflow import ExtractDocumentFeaturesOperator
from eagle.models.masters import EDINETDocument
from eagle.models import NumberOfExecutives


DEFAULT_DATE = timezone.datetime(2018, 9, 10)
# https://disclosure.edinet-fsa.go.jp/api/v1/documents.json?type=2&date=2018-09-10


class TestExtractDocumentFeaturesOperator(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        configuration.load_test_config()
        dag_id = "extract_document_feature_prepare_dag"
        cls.prepare_dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)

        get_list = GetEDINETDocumentListOperator(
                task_id="get_document_list", dag=cls.prepare_dag)
        get_document = GetEDINETDocumentSensor(
                max_retrieve=3, document_ids=("S100E2NM", "S100E2S2"),
                task_id="get_document", dag=cls.prepare_dag, poke_interval=2)
        register_document = RegisterDocumentOperator(
                task_id="register_document", dag=cls.prepare_dag)
        cls.prepare_dag.clear()
        get_list.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        get_document.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        register_document.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

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
            "extract_document_feature",
            default_args={
                "owner": "airflow_ml",
                'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)

    def test_extract_document_feature(self):
        task = ExtractDocumentFeaturesOperator(
                report_kinds=("annual",),
                task_id="extract_feature", dag=self.dag)

        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                 ignore_ti_state=True)

        documents = EDINETDocument.objects.all()
        self.assertGreater(len(documents), 0)
        for d in documents:
            self.assertTrue(
                NumberOfExecutives.objects.filter(document=d).exists())
