import tempfile
import importlib
from datetime import datetime
import calendar
from django.db import transaction
import edinet
from eagle.models import EDINETCompany, EDINETDocument


class EDINETFeatureUpdater():

    def __init__(self, storage):
        self.storage = storage

    def update_from_annual_report(self,
                                  date, feature,
                                  jcn="", edinet_document_id="",
                                  use_pdf=False, dryrun=False):
        # annual_report = ("120", "130")
        annual_report = ("120",)  # 130 does not have period start/end
        return self.update_from_document(
                    date, feature, annual_report,
                    jcn, edinet_document_id,
                    use_pdf, dryrun)

    def ym_to_num(self, date):
        return date.year * 100 + date.month

    def update_from_document(self,
                             date, feature,
                             target_document_type,
                             jcn="", edinet_document_id="",
                             use_pdf=False, dryrun=False):

        documents = []
        if jcn:
            company = EDINETCompany.objects.get(jcn=jcn)
            documents = EDINETDocument.objects.filter(
                company=company,
                period_start__lte=date,
                period_end__gte=date,
                edinet_document_type__in=target_document_type,
                ordinance_code="010")  # 企業内容等の開示に関する内閣府令
        elif edinet_document_id:
            documents = EDINETDocument.objects.filter(
                edinet_document_id=edinet_document_id)
        else:
            documents = EDINETDocument.objects.filter(
                period_start__lte=date,
                period_end__gte=date,
                edinet_document_type__in=target_document_type,
                ordinance_code="010")

        task = []
        for d in documents:
            objects = []
            with tempfile.NamedTemporaryFile(delete=True) as f:
                if use_pdf:
                    self.storage.download_file(d.pdf_path, f.name)
                else:
                    self.storage.download_file(d.xbrl_path, f.name)

                aspect, _feature = feature.split(".")
                value = edinet.parse(f.name, aspect, _feature)
                _class = self.get_feature_object(_feature)

                for ym in self.month_iterator(d):
                    if self.ym_to_num(date) <= self.ym_to_num(ym):
                        object = _class(
                            company=d.company,
                            year_month=ym,
                            unit=value.unit,
                            value=value.value,
                            document=d,
                            ground=value.ground
                        )
                        objects.append(object)

                task.append(objects)

        if not dryrun:
            for t in task:
                with transaction.atomic():
                    for o in t:
                        _class.objects.save(o)
        return task

    def month_iterator(self, document):
        ym = datetime(document.period_start.year,
                      document.period_start.month,
                      document.period_start.day)

        end = document.period_end
        while self.ym_to_num(ym) <= self.ym_to_num(end):
            yield ym
            next_y, next_m = calendar.nextmonth(ym.year, ym.month)
            ym = datetime(next_y, next_m, 1)

    def get_feature_object(self, feature_name):
        _class = None
        try:
            module = importlib.import_module("eagle.models.features")

            def to_camel(snake_str):
                components = snake_str.split("_")
                return "".join(x.title() for x in components)

            _class_name = to_camel(feature_name)
            _class = getattr(module, _class_name)

        except Exception as ex:
            raise Exception(f"Can't load class that matches {feature_name} \n {ex}.")

        return _class
