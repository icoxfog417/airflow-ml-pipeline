import tempfile
import importlib
from datetime import datetime
import calendar
from django.db import transaction
import edinet


class EDINETFeatureExtractor():

    def __init__(self, storage):
        self.storage = storage

    def extract_feature(self, document, feature_or_features,
                        dryrun=False):

        features = []
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
