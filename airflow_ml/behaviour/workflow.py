from datetime import datetime, timedelta
import time
import random
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.python_operator import PythonOperator
from pprint import pprint


class ShowDateOperator(BaseOperator):

    def execute(self, context):
        start_date = self.start_date.strftime("%Y/%m/%d")
        execution_date = context['ti'].execution_date.strftime("%Y/%m/%d")

        self.log.info(f"Start Date is {start_date}")
        self.log.info(f"Execution Date is {execution_date}")

        return {
            "start_date": start_date,
            "execution_date": execution_date
        }


class StatusOperator(BaseOperator):

    @apply_defaults
    def __init__(self, status, *args, **kwargs):
        self.status = status
        super().__init__(*args, **kwargs)

    def execute(self, context):
        if self.status:
            self.log.info("Execution Success.")
        else:
            raise Exception("Task is fail")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 4, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


dag = DAG("airflow-ml-behaviour", default_args=default_args,
          schedule_interval="@daily")

# print_task > sleep_task
show_date_task = ShowDateOperator(task_id="show_date", dag=dag)
success_task_1 = StatusOperator(status=True,
                                task_id="success_task_1", dag=dag)
success_task_2 = StatusOperator(status=True,
                                task_id="success_task_2", dag=dag)

show_date_task >> success_task_1 >> success_task_2
