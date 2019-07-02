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

    def get_file_path(self, document):
        if document.submitted_date is None:
            raise Exception(document.document_id)
        prefix = self.document_path_at(document.submitted_date)
        files = self.storage.list_objects(prefix)
        result = {
            "xbrl": "",
            "pdf": ""
        }
        for f in files:
            name = os.path.basename(f)
            if name.startswith(document.document_id):
                if name.endswith(".xbrl"):
                    result["xbrl"] = f
                elif name.endswith(".pdf"):
                    result["pdf"] = f

        return result


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
        if not self.storage.exists(self.list_path_at(execution_date)):
            self.log.info("Document @ {} does not found.".format(
                           execution_date.strftime("%Y/%m/%d")))
            return True
        elif len(self._targets) == 0:
            documents = self.get_documents_at(execution_date)

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
                    name, ext = os.path.splitext(os.path.basename(content_path))
                    name = name.split("__")[0]
                    name = name.split("_")[0]
                    file_name = name + ext
                    path = self.document_path_at(execution_date, file_name)
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


class RegisterDocumentOperator(BaseOperator, EDINETMixin):
    """ Register documents from stored list and documents."""

    @apply_defaults
    def __init__(self, max_retrieve=-1, document_types=(),
                 *args, **kwargs):
        self.max_retrieve = max_retrieve
        self.document_types = document_types
        self._targets = []
        super().__init__(*args, **kwargs)

    def execute(self, context):
        execution_date = context["execution_date"]
        if not self.storage.exists(self.list_path_at(execution_date)):
            self.log.info("Document @ {} does not found.".format(
                           execution_date.strftime("%Y/%m/%d")))
            return False

        documents = self.get_documents_at(execution_date)

        if len(self.document_types) == 0:
            self._targets = documents.list
        else:
            self._targets = [d for d in documents.list
                             if d.doc_type_code in self.document_types]

        if self.max_retrieve > 0:
            self._targets = self._targets[:self.max_retrieve]

        service = EDINETDocumentRegister()

        registered = 0
        for d in self._targets:
            path = self.get_file_path(d)
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
            service.extract_from_documents(r, self.features,
                submitted_date=execution_date)


class UpdateCompanyDataOperator(BaseOperator, EDINETMixin):
    """ Update company data from feature of documents """

    @apply_defaults
    def __init__(self, report_kinds, features=(),
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

        service = CompanyDataUpdater()
        companies = EDINETCompany.objects.all()
        date = datetime(
            execution_date.year, execution_date.month, execution_date.day)
        for r in self.report_kinds:
            if r == "annual":
                date = datetime(date.year - 1, date.month, date.day)
            else:
                self.log.info("Report kind {} does not supported".format(r))
                return False

            for c in companies:
                service.update_company_data(c, date, self.features)

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
