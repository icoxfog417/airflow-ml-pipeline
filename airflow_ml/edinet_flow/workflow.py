import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
import json
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow_ml.edinet_flow.storage import Storage
from edinet.client.document_list_client import BaseDocumentListClient
import edinet


class EDINETMixin():
    BUCKET = "edinet-data-store"

    @property
    def storage(self):
        return Storage(self.BUCKET)

    def list_name_at(self, date):
        file_name = f"{date.strftime('%Y-%m-%d')}.json"
        return file_name

    def list_path_of(self, date):
        return "lists/{}".format(self.list_name_at(date))

    def document_path_of(self, date, file_name=""):
        if file_name:
            return f"documents/{date.strftime('%Y-%m-%d')}/{file_name}"
        else:
            return f"documents/{date.strftime('%Y-%m-%d')}"


class GetEDINETDocumentListOperator(BaseOperator, EDINETMixin):
    """ Save EDINET Document list response to GCS."""

    def execute(self, context):
        execution_date = context["execution_date"]
        self.log.info("Get Document list of @ {}.".format(
            execution_date.strftime("%Y/%m/%d")))

        client = BaseDocumentListClient(response_type="2")
        body = client._get(execution_date)
        path = self.list_path_of(execution_date)
        self.storage.upload_file(path, content=body)
        return path


class GetEDINETDocumentSensor(BaseSensorOperator, EDINETMixin):
    """ Save files from stored document list file."""

    @apply_defaults
    def __init__(self, max_retrieve=-1, document_types=(),
                 file_types=("xbrl", "pdf"), *args, **kwargs):
        self.max_retrieve = max_retrieve
        self.document_types = document_types
        self.file_types = file_types
        self._index = 0
        self._targets = []
        super().__init__(*args, **kwargs)

    def poke(self, context):
        execution_date = context["execution_date"]

        if not self.storage.exists(self.list_path_of(execution_date)):
            self.log.info("Document @ {} does not found.".format(
                           execution_date.strftime("%Y/%m/%d")))
            return True
        elif len(self._targets) == 0:
            document_list = self.storage.download_conent(
                                self.list_path_of(execution_date))
            document_list = json.loads(document_list)
            documents = edinet.models.Documents.create(document_list)

            if len(self.document_types) == 0:
                self._targets = documents.list
            else:
                self._targets = [d for d in documents.list
                                 if d.doc_type_code in self.document_types]

        if len(self._targets) == 0:
            self.log.info("No target document exist @ {}.".format(
                           execution_date.strftime("%Y/%m/%d")))

            return True

        document = self._targets[self._index]

        for file_type in self.file_types:
            content_path = ""
            try:
                if file_type == "xbrl" and document.has_xbrl:
                    content_path = document.get_xbrl()
                elif file_type == "pdf" and document.has_pdf:
                    content_path = document.get_pdf()

                if content_path:
                    file_name = os.path.basename(content_path)
                    file_name = str(file_name).split("__")[-1]
                    path = self.document_path_of(execution_date, file_name)
                    self.storage.upload_file(path, content_path=content_path)

            except Exception as ex:
                self.log.error(ex)

        self._index += 1

        if self.max_retrieve > 0:
            if self._index < self.max_retrieve:
                return False
            else:
                return True
        elif self._index < len(self._targets):
            return False
        else:
            return True


"""

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
    def __init__(self, document_type="xbrl", *args, **kwargs):
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
        default_path = os.path.join(os.path.dirname(__file__), "../../data")

        gcp_bucket = Variable.get("gcp_bucket", default_var="")
        save_dir = default_path if not gcp_bucket else ""

        document_id = document_ids[self._next_document_index]
        self.log.info("Dealing {}/{} documents.".format(
                        (self._next_document_index + 1), len(document_ids)))
        if self.document_type == "pdf":
            path = client.get_pdf(document_id, save_dir)
        else:
            path = client.get_xbrl(document_id, save_dir)

        if gcp_bucket:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.get_bucket(gcp_bucket)
            exact_name = str(path).split("__")[-1]
            bucket.blob(exact_name).upload_from_filename(filename=str(path))

        self._next_document_index += 1

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


edinet_get_document_list = EDINETGetDocumentsOperator(
                                filter_func=lambda i, d: i < 3,
                                task_id="edinet_get_document_list",
                                provide_context=True,
                                dag=dag)
edinet_get_document = EDINETGetDocumentSensor(
                                task_id="edinet_get_document",
                                poke_interval=60,
                                dag=dag)

edinet_get_document_list >> edinet_get_document
"""
