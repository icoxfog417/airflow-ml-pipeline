import tempfile
import importlib
from datetime import datetime
import calendar
from django.db import transaction
import edinet
from eagle.models import EDINETCompany, EDINETDocument


class CompanyDataMerger():

    def update_company_data(self,
                            company, date,
                            feature_or_features,
                            dryrun=False):

        documents = EDINETDocument.objects.filter(
            period_start__lte=date,
            period_end__gte=date)

        task = []
        for d in documents:
            objects = []
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

    def ym_to_num(self, date):
        return date.year * 100 + date.month

    def month_iterator(self, document):
        ym = datetime(document.period_start.year,
                      document.period_start.month,
                      document.period_start.day)

        end = document.period_end
        while self.ym_to_num(ym) <= self.ym_to_num(end):
            yield ym
            next_y, next_m = calendar.nextmonth(ym.year, ym.month)
            ym = datetime(next_y, next_m, 1)

    def update_from_documents(self, feature, documents,
                              use_pdf=False, dryrun=False):

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
