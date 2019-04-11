import unittest
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from airflow_ml.behaviour.workflow import ShowDateOperator


class TestShowDateOperator(unittest.TestCase):

    def test_execute(self):
        dummy_dag = DAG(dag_id="show_date_operator_test_dag",
                        start_date=datetime(2019, 1, 5))
        task = ShowDateOperator(dag=dummy_dag, task_id="test_task")
        execution_date = datetime.now()
        ti = TaskInstance(task=task, execution_date=execution_date)
        result = task.execute(ti.get_template_context())

        self.assertEqual(result["start_date"], "2019/01/05")
        self.assertEqual(result["execution_date"], execution_date.strftime("%Y/%m/%d"))
