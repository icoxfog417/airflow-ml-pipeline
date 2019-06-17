import os
import unittest
from datetime import datetime
from airflow import DAG, configuration
from airflow.models import TaskInstance
from airflow.utils import timezone
from airflow_ml.edinet_flow.workflow import GetEDINETDocumentListOperator


DEFAULT_DATE = timezone.datetime(2017, 6, 1)


class TestGetEDINETDocumentListOperator(unittest.TestCase):

    def setUp(self):
        super().setUp()
        configuration.load_test_config()
        self.dag = DAG(
            "get_edinet_dl_dag",
            default_args={
                "owner": "airflow_ml",
                'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)

    def test_execute(self):
        task = GetEDINETDocumentListOperator(
                task_id="get_edinet_dl", dag=self.dag)
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        path = task.execute(ti.get_template_context())
        self.assertTrue(task.storage.exists(path))
        file_name = os.path.basename(path)
        self.assertEqual(file_name, "{}.json".format(
                         DEFAULT_DATE.strftime("%Y-%m-%d")))
        task.storage.delete(path)
