import re
from pathlib import Path
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


class BaseDocumentListClient(Client):

    def __init__(self, response_type):
        super().__init__(target="documents.json")
        self.response_type = response_type

    def get(self, date):
        url = self.endpoint
        _date = date
        if isinstance(date, str):
            try:
                _date = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise Exception("Date format should be yyyy-mm-dd.")

        _date = _date.strftime("%Y-%m-%d")

        params = {
            "date": _date,
            "type": self.response_type
        }

        r = requests.get(url, params=params, verify=False)  # Caution

        if not r.ok:
            r.raise_for_status()
        else:
            body = r.json()
            return self.parse(body)

    def parse(self, body):
        raise NotImplementedError("You have to implement parse method.")


class MetaDataClient(BaseDocumentListClient):

    def __init__(self):
        super().__init__(response_type="1")

    def parse(self, body):
        instance = model.MetaData.create(body)
        return instance


class DocumentListClient(BaseDocumentListClient):

    def __init__(self):
        super().__init__(response_type="2")

    def parse(self, body):
        items = body["results"]
        instances = [model.Document.create(item) for item in items]
        return instances


class DocumentClient(Client):

    def __init__(self):
        super().__init__(target="documents/{}")

    def get(self, save_dir, document_id, response_type, file_name=""):
        save_path = Path(save_dir)
        if not save_path.exists():
            raise Exception("Save directory does not exist.")

        url = self.endpoint.format(document_id)
        params = {
            "type": response_type
        }

        r = requests.get(url, params=params, stream=True, verify=False)  # Caution

        if not r.ok:
            r.raise_for_status()
        else:
            _file_name = file_name
            if not _file_name:
                if "content-disposition" in r.headers:
                    d = r.headers["content-disposition"]
                    file_names = re.findall("filename=\"(.+)\"", d)
                    if len(file_names) > 0:
                        _file_name = file_names[0]

                if not _file_name:
                    ext = ".pdf" if response_type == "2" else ".zip"
                    _file_name = document_id + ext

            chunk_size = 1024
            save_path = save_path.joinpath(_file_name)
            with save_path.open(mode="wb") as f:
                for chunk in r.iter_content(chunk_size):
                    f.write(chunk)

            return save_path
