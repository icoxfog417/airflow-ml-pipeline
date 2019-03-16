# airflow-ml-exercises

The repository to learn Machine Learning with Airflow

## Setup

Set `AIRFLOW_HOME` to current folder (if you need).  
This setting is required when you execute `airflow` commands.

```
mkdir airflow
export AIRFLOW_HOME="$(pwd)/airflow"
```

Then, install airflow ([without GPL libraries](https://github.com/apache/airflow/pull/3660)).  
About Python 3.7 problem: [Update Tenacity to 4.12](https://github.com/apache/airflow/pull/3723)

```
SLUGIFY_USES_TEXT_UNIDECODE=yes pip install apache-airflow tenacity==4.12.0 --no-binary=python-slugify
```

When pipenv.

```
PIP_NO_BINARY=python-slugify SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv install apache-airflow tenacity==4.12.0
```

Initialize the database.

```
airflow initdb
```

Now, we change the default `dag` folder. So let's change `dags_folder` setting in the `airflow/airflow.cfg`.

```
dags_folder = /mnt/c/Users/ico/Documents/works/airflow-ml-exercises/tutorial
```

Run web server.

```
airflow webserver --port 8080
```

