from datetime import datetime
import requests
import airflow_ml.edinet.models as model


class Client():
    BASE_URL = "https://disclosure.edinet-fsa.go.jp/api/{}/{}"

    def __init__(self, target, version="v1"):
        self.version = version
        self.target = target

    @property
    def endpoint(self):
        return self.BASE_URL.format(self.version, self.target)


class DocumentListClient(Client):

    def __init__(self):
        super().__init__(target="documents.json")

    def _get(self, date, response_type):
        url = self.endpoint
        print(url)
        _date = date
        if isinstance(date, str):
            try:
                _date = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise Exception("Date format should be yyyy-mm-dd.")

        _date = _date.strftime("%Y-%m-%d")

        params = {
            "date": _date,
            "type": response_type
        }

        r = requests.get(url, params=params, verify=False)  # Caution

        if not r.ok:
            r.raise_for_status()
        else:
            body = r.json()
            if response_type == "1":
                instance = model.MetaData.create(body)
                return instance
            else:
                items = body["results"]
                instances = [model.Document.create(item) for item in items]
                return instances


class MetaDataClient(DocumentListClient):

    def get(self, date):
        return super()._get(date, response_type="1")


class DocumentClient(DocumentListClient):

    def get(self, date):
        return super()._get(date, response_type="2")
