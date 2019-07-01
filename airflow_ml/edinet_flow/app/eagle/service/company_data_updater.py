import importlib
from datetime import datetime
import calendar
from django.db import transaction
from eagle.models import EDINETDocument
from eagle.models import CompanyData


class CompanyDataUpdater():

    def update_company_data(self,
                            company, date,
                            feature_or_features,
                            dryrun=False):

        features = []
        if isinstance(feature_or_features, str):
            features = [feature_or_features]

        updates = {}
        for f in features:
            aspect, _feature = f.split(".")
            _class = self.get_feature_object(_feature)

            related_documents = _class.objects.filter(
                document__company=company,
                document__period_start__lte=date,
                document__period_end__gte=date
                ).values_list("document", flat=True).distinct()
            related_documents = EDINETDocument.objects.filter(
                pk__in=related_documents
            )

            year_month = {}

            for d in related_documents:
                value = _class.objects.get(document=d)

                for ym in self.month_iterator(d):
                    if self.ym_to_num(date) <= self.ym_to_num(ym):
                        object = CompanyData(
                            company=d.company,
                            year_month=ym,
                        )
                        setattr(object, _feature, value)
                        year_month[ym] = object

            updates[f] = year_month

        if not dryrun:
            for f in updates:
                with transaction.atomic():
                    for ym in updates[f]:
                        object = updates[f][ym]
                        object.__class__.objects.save(object)
        return updates

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
