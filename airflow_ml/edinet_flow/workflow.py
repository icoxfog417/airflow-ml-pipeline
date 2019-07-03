import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "./app/"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.settings")
import json
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow_ml.edinet_flow.storage import Storage
import edinet
from edinet.client.document_list_client import BaseDocumentListClient

import django
django.setup()
from eagle.models import EDINETCompany
from eagle.service import EDINETDocumentRegister
from eagle.service import EDINETFeatureExtractor
from eagle.service import CompanyDataUpdater


class EDINETMixin():
    BUCKET = "chakki.edi-test.jp"

    @property
    def storage(self):
        return Storage.s3(self.BUCKET)

    def list_name_at(self, date):
        file_name = f"{date.strftime('%Y-%m-%d')}.json"
        return file_name

    def list_path_at(self, date):
        return "lists/{}".format(self.list_name_at(date))

    def get_documents_at(self, date):
        path = self.list_path_at(date)
        document_list = self.storage.download_conent(path)
        document_list = json.loads(document_list)
        documents = edinet.models.Documents.create(document_list)
        return documents

    def document_path_at(self, date, file_name=""):
        if file_name:
            return f"documents/{date.strftime('%Y-%m-%d')}/{file_name}"
        else:
            return f"documents/{date.strftime('%Y-%m-%d')}"

    def get_files_at(self, date):
        prefix = self.document_path_at(date)
        files = self.storage.list_objects(prefix)

        file_paths = {}
        for f in files:
            name = os.path.basename(f)
            root, ext = os.path.splitext(name)
            if root not in file_paths:
                file_paths[root] = {
                    "xbrl": "",
                    "pdf": ""
                }
            if ext == ".xbrl":
                file_paths[root]["xbrl"] = f
            elif ext == ".pdf":
                file_paths[root]["pdf"] = f

        return file_paths

    def get_file_of(self, document):
        files = self.get_files_at(document.submitted_date)
        if document.document_id in files:
            return files[document.document_id]
        else:
            return {}


class GetEDINETDocumentListOperator(BaseOperator, EDINETMixin):
    """ Save EDINET Document list response to GCS."""

    def execute(self, context):
        execution_date = context["execution_date"]
        self.log.info("Get Document list of @ {}.".format(
            execution_date.strftime("%Y/%m/%d")))

        client = BaseDocumentListClient(response_type="2")
        body = client._get(execution_date)
        path = self.list_path_at(execution_date)
        self.storage.upload_file(path, content=body)
        return path


class GetEDINETDocumentSensor(BaseSensorOperator, EDINETMixin):
    """ Save files from stored document list file."""

    @apply_defaults
    def __init__(self, document_ids=(), max_retrieve=-1, *args, **kwargs):
        self._document_ids = document_ids
        self._max_retrieve = max_retrieve
        self._index = 0
        self._targets = []
        super().__init__(*args, **kwargs)

    def poke(self, context):
        execution_date = context["execution_date"]
        if not self.storage.exists(self.list_path_at(execution_date)):
            self.log.info("Document @ {} does not found.".format(
                           execution_date.strftime("%Y/%m/%d")))
            return True
        elif len(self._targets) == 0:
            documents = self.get_documents_at(execution_date)
            self._targets = documents.list
            if len(self._document_ids) > 0:
                self._targets = [d for d in self._targets
                                 if d.document_id in self._document_ids]

            if self._max_retrieve > 0:
                self._targets = self._targets[:self._max_retrieve]

        if len(self._targets) == 0:
            self.log.info("No target document exist @ {}.".format(
                           execution_date.strftime("%Y/%m/%d")))

            return True

        document = self._targets[self._index]

        for file_type in ("xbrl", "pdf"):
            content_path = ""
            try:
                if file_type == "xbrl" and document.has_xbrl:
                    content_path = document.get_xbrl()
                elif file_type == "pdf" and document.has_pdf:
                    content_path = document.get_pdf()

                if content_path:
                    name, ext = os.path.splitext(os.path.basename(content_path))
                    name = name.split("__")[0]
                    name = name.split("_")[0]
                    file_name = name + ext
                    path = self.document_path_at(execution_date, file_name)
                    self.storage.upload_file(path, content_path=content_path)

            except Exception as ex:
                self.log.error(ex)

        self._index += 1
        if self._index == len(self._targets):
            self._targets.clear()
            self._index = 0
            return True
        else:
            return False


class RegisterDocumentOperator(BaseOperator, EDINETMixin):
    """ Register documents from stored list and documents."""

    @apply_defaults
    def __init__(self, max_retrieve=-1,
                 document_types=(), ordinance_code=(), withdraw_status="",
                 *args, **kwargs):
        self.max_retrieve = max_retrieve
        self._document_types = document_types
        self._ordinance_code = ordinance_code
        self._withdraw_status = withdraw_status
        super().__init__(*args, **kwargs)

    def execute(self, context):
        execution_date = context["execution_date"]
        if not self.storage.exists(self.list_path_at(execution_date)):
            self.log.info("Document @ {} does not found.".format(
                           execution_date.strftime("%Y/%m/%d")))
            return False

        files = self.get_files_at(execution_date)
        documents = self.get_documents_at(execution_date)
        self._targets = [d for d in documents.list
                         if d.document_id in files]

        if len(self._document_types) > 0:
            self._targets = [d for d in self._targets
                             if d.doc_type_code in self._document_types]

        if len(self._ordinance_code) > 0:
            self._targets = [d for d in self._targets
                             if d.ordinance_code in self._ordinance_code]

        if len(self._withdraw_status) > 0:
            self._targets = [d for d in self._targets
                             if d.withdraw_status == self._withdraw_status]

        if self.max_retrieve > 0:
            self._targets = self._targets[:self.max_retrieve]

        service = EDINETDocumentRegister()
        registered = 0
        for d in self._targets:
            path = self.get_file_of(d)
            service.register_document(d, path["xbrl"], path["pdf"])
            registered += 1

        self.log.info("{} documents are registered.".format(registered))


class ExtractDocumentFeaturesOperator(BaseOperator, EDINETMixin):
    """ Extract feature of registered document """

    @apply_defaults
    def __init__(self, report_kinds=(), features=(),
                 max_retrieve=-1, *args, **kwargs):
        self.report_kinds = report_kinds
        self.features = features
        super().__init__(*args, **kwargs)

    def execute(self, context):
        execution_date = context["execution_date"]
        if not self.storage.exists(self.list_path_at(execution_date)):
            self.log.info("Document @ {} does not found.".format(
                           execution_date.strftime("%Y/%m/%d")))
            return False

        service = EDINETFeatureExtractor(self.storage)
        for r in self.report_kinds:
            service.extract_from_documents(
                r, self.features,
                submitted_date=execution_date)


class UpdateCompanyDataOperator(BaseOperator, EDINETMixin):
    """ Update company data from feature of documents """

    @apply_defaults
    def __init__(self, features=(),
                 max_retrieve=-1, *args, **kwargs):
        self.features = features
        super().__init__(*args, **kwargs)

    def execute(self, context):
        execution_date = context["execution_date"]
        if not self.storage.exists(self.list_path_at(execution_date)):
            self.log.info("Document @ {} does not found.".format(
                           execution_date.strftime("%Y/%m/%d")))
            return False

        service = CompanyDataUpdater()
        service.update_by_submitted_date(execution_date, self.features)


"""

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
