import tempfile
import importlib
import datetime
from django.db import transaction
import edinet
from eagle.models import EDINETDocument


class EDINETFeatureExtractor():

    def __init__(self, storage):
        self.storage = storage

    @property
    def annual_features(cls):
        return [
            "executive_state.number_of_executives"
        ]

    def extract_from_documents(self, report_kind, features=(),
                               submitted_date=None,
                               dryrun=False):
        _features = []
        documents = []
        if report_kind == "annual":
            _features = self.annual_features
            documents = EDINETDocument.objects.filter(
                edinet_document_type__in=(120, 130),
                withdraw_status="0",
                ordinance_code="010",
                submitted_date__range=(
                    datetime.datetime.combine(submitted_date, datetime.time.min),
                    datetime.datetime.combine(submitted_date, datetime.time.max)
                )
            )

        if len(features) > 0:
            _features = [f for f in _features in features]

        extracteds = []
        for d in documents:
            e = self.extract_feature(d, _features, dryrun=dryrun)
            extracteds.append(e)

        return extracteds

    def extract_feature(self, document, feature_or_features,
                        dryrun=False):

        features = feature_or_features
        if isinstance(feature_or_features, str):
            features = [feature_or_features]

        feature_objects = {}
        with tempfile.NamedTemporaryFile(delete=True) as f:
            self.storage.download_file(document.path, f.name)
            for _f in features:
                aspect, _feature = _f.split(".")
                value = edinet.parse(f.name, aspect, _feature)
                _class = self.get_feature_object(_feature)

                model = _class(
                    document=document,
                    unit=value.unit,
                    value=value.value,
                    ground=value.ground
                )
                feature_objects[_f] = model

        if not dryrun:
            with transaction.atomic():
                for k in feature_objects:
                    object = feature_objects[k]
                    object.save()

        return feature_objects

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
