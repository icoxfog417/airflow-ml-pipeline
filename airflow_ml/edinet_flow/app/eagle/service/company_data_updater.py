import importlib
import calendar
import datetime
from django.db import transaction
from eagle.models import EDINETDocument
from eagle.models import CompanyData


class CompanyDataUpdater():

    def update_by_company_date(self,
                               company, date,
                               features=(),
                               dryrun=False):

        def filter_func(_class):
            documents = _class.objects.filter(
                document__company=company,
                document__period_end__gte=date
                ).values_list("document", flat=True).distinct()
            documents = EDINETDocument.objects.filter(
                pk__in=documents
            )
            return documents

        return self._update(filter_func, features, date, dryrun)

    def update_by_submitted_date(self,
                                 submitted_date,
                                 features=(),
                                 dryrun=False):

        def filter_func(_class):
            documents = _class.objects.filter(
                document__submitted_date__range=(
                    datetime.datetime.combine(submitted_date, datetime.time.min),
                    datetime.datetime.combine(submitted_date, datetime.time.max)
                )).values_list("document", flat=True).distinct()
            documents = EDINETDocument.objects.filter(
                pk__in=documents
            )
            return documents

        return self._update(filter_func, features, dryrun=dryrun)

    def _update(self, filter_func, features=(), from_date=None, dryrun=False):

        _features = CompanyData._meta.get_fields()
        _features = [f.name for f in _features
                     if f.name not in ("company", "year_month", "id")]

        if len(features) > 0:
            _features = [f for f in _features if f in features]

        updates = {}
        for f in _features:
            _class = self.get_feature_object(f)
            related_documents = filter_func(_class)

            year_month = {}
            for d in related_documents:
                value = _class.objects.get(document=d)

                for ym in self.month_iterator(d):
                    if from_date is not None and\
                       self.ym_to_num(from_date) > self.ym_to_num(ym):
                        continue

                    object = CompanyData(
                        company=d.company,
                        year_month=ym,
                    )
                    setattr(object, f, value)
                    year_month[ym] = object

            updates[f] = year_month

        if not dryrun:
            for f in updates:
                with transaction.atomic():
                    for ym in updates[f]:
                        object = updates[f][ym]
                        object.save(object)
        return updates

    def ym_to_num(self, date):
        return date.year * 100 + date.month

    def month_iterator(self, document):
        ym = datetime.datetime(document.period_start.year,
                               document.period_start.month,
                               document.period_start.day)

        end = document.period_end
        while self.ym_to_num(ym) <= self.ym_to_num(end):
            yield ym
            next_y, next_m = calendar.nextmonth(ym.year, ym.month)
            ym = datetime.datetime(next_y, next_m, 1)

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
