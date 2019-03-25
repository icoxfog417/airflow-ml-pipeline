# airflow-ml-exercises

The repository to learn Machine Learning with Airflow

## Setup

Set `AIRFLOW_HOME` to current folder (if you need).  
This setting is required when you execute `airflow` commands (recommend to use [.env](https://pipenv.readthedocs.io/en/latest/advanced/#automatic-loading-of-env) file).

```
mkdir airflow
export AIRFLOW_HOME="$(pwd)/airflow"
```

Then, install airflow ([without GPL libraries](https://github.com/apache/airflow/pull/3660)).  
About Python 3.7 problem: [Update Tenacity to 4.12](https://github.com/apache/airflow/pull/3723)

```
SLUGIFY_USES_TEXT_UNIDECODE=yes pip install apache-airflow tenacity==4.12.0 python-dotenv --no-binary=python-slugify
```

When pipenv.

```
PIP_NO_BINARY=python-slugify SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv install apache-airflow tenacity==4.12.0 python-dotenv
```

Initialize the database.

```
airflow initdb
```

Now, we change the default `dag` folder. So let's change `dags_folder` setting in the `airflow/airflow.cfg`.

```
dags_folder = /your_folder/airflow-ml-exercises/airflow-ml
```

Run web server.

```
airflow webserver --port 8080
```

If you want to refresh list of DAGs, execute following command.

```
python -c "from airflow.models import DagBag; d = DagBag();"
```

