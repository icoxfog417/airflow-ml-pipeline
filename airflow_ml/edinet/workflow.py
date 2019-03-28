import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import airflow_ml.edinet.api as api


class EDINETGetDocumentsOperator(BaseOperator):

    @apply_defaults
    def __init__(self, filter_func=None, *args, **kwargs):
        self.filter_func = filter_func
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info("Retreave list of documents from EDINET @ {}.".format(
            self.start_date.strftime("%Y/%m/%d")))

        client = api.DocumentListClient()
        documents = client.get(self.start_date)
        total_count = len(documents)
        if self.filter_func:
            documents = [d for i, d in enumerate(documents)
                         if self.filter_func(i, d)]

        self.log.info("Dealing {}/{} documents.".format(
                        len(documents), total_count))

        document_ids = [d.document_id for d in documents]
        return document_ids


class EDINETGetDocumentSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, save_dir, document_type="xbrl", *args, **kwargs):
        self.save_dir = save_dir
        self.document_type = document_type
        self._next_document_index = -1
        super().__init__(*args, **kwargs)

    def poke(self, context):
        document_ids = context["task_instance"].xcom_pull(
                        task_ids="edinet_get_document_list")

        if self._next_document_index < 0:
            self.log.info("Download the {} documents from EDINET.".format(
                            len(document_ids)))
            self._next_document_index = 0

        client = api.DocumentClient()
        document_id = document_ids[self._next_document_index]
        self.log.info("Dealing {}/{} documents.".format(
                        (self._next_document_index + 1), len(document_ids)))
        if self.document_type == "pdf":
            client.get_pdf(self.save_dir, document_id)
        else:
            client.get_xbrl(self.save_dir, document_id)

        self._next_document_index +=1

        if self._next_document_index == len(document_ids):
            return True
        else:
            return False


yesterday = datetime.today() - timedelta(1)

default_args = {
    "owner": "chakki",
    "depends_on_past": False,
    "start_date": yesterday,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay":timedelta(minutes=5)
}


dag = DAG("airflow-ml-edinet", default_args=default_args)
default_path = os.path.join(os.path.dirname(__file__), "../../data")
SAVE_DIR = os.getenv("SAVE_DIR", default_path)


edinet_get_document_list = EDINETGetDocumentsOperator(
                                filter_func=lambda i, d: i < 3,
                                task_id="edinet_get_document_list",
                                provide_context=True,
                                dag=dag)
edinet_get_document = EDINETGetDocumentSensor(
                                save_dir=SAVE_DIR,
                                task_id="edinet_get_document",
                                poke_interval=60,
                                dag=dag)

edinet_get_document_list >> edinet_get_document
