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

        class_models = []
        with tempfile.NamedTemporaryFile(delete=True) as f:
            self.storage.download_file(document.path, f.name)
            for f in features:
                aspect, _feature = f.split(".")
                value = edinet.parse(f.name, aspect, _feature)
                _class = self.get_feature_object(_feature)

                model = _class(
                    document=document,
                    unit=value.unit,
                    value=value.value,
                    ground=value.ground
                )
                class_models.append((_class, model))

        if not dryrun:
            with transaction.atomic():
                for c, m in class_models:
                    c.objects.save(m)

        models = [m for c, m in class_models]
        return models

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
