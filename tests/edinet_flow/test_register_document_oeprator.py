import os
import json
from django.test import TestCase
from airflow import DAG, configuration
from airflow.utils import timezone
from airflow_ml.edinet_flow.workflow import EDINETMixin
from airflow_ml.edinet_flow.workflow import GetEDINETDocumentListOperator
from airflow_ml.edinet_flow.workflow import GetEDINETDocumentSensor
from airflow_ml.edinet_flow.workflow import RegisterDocumentOperator


DEFAULT_DATE = timezone.datetime(2019, 6, 7)


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
                max_retrieve=3, document_types=("120"),
                task_id="get_document", dag=cls.prepare_dag, poke_interval=2)

        get_list >> get_document
        get_document.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                         ignore_ti_state=True)

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
        pass
