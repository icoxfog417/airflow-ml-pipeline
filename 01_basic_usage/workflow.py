from datetime import datetime, timedelta
import time
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pprint import pprint


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

dag = DAG("01_basic_usage", default_args=default_args)


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return "Print to Log"


print_task = PythonOperator(
    task_id="print_task",
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)


def sleep(seconds):
    time.sleep(seconds)


def make_sleep_task(task_name, dag):
    seconds = random.randint(1, 3)
    task = PythonOperator(
        task_id=task_name,
        python_callable=sleep,
        op_kwargs={"seconds": seconds},
        dag=dag,
    )
    return task


# print_task > sleep_task
print_task.set_downstream(make_sleep_task("last_sleep", dag))

# sleep_task > print_task
make_sleep_task("first_sleep", dag) >> print_task
