# Airflow for data analytics pipeline

Research project to build data analytics pipeline by Airflow.

## Research

1. [Introduction of data analytics pipeline](https://medium.com/programming-soda/apache-airflow%E3%81%A7%E3%82%A8%E3%83%B3%E3%83%89%E3%83%A6%E3%83%BC%E3%82%B6%E3%83%BC%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92%E3%83%91%E3%82%A4%E3%83%97%E3%83%A9%E3%82%A4%E3%83%B3%E3%82%92%E6%A7%8B%E7%AF%89%E3%81%99%E3%82%8B-part0-f5cce5da628)
2. [Case study of data analytics pipeline](https://medium.com/programming-soda/apache-airflow%E3%81%A7%E3%82%A8%E3%83%B3%E3%83%89%E3%83%A6%E3%83%BC%E3%82%B6%E3%83%BC%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92%E3%83%91%E3%82%A4%E3%83%97%E3%83%A9%E3%82%A4%E3%83%B3%E3%82%92%E6%A7%8B%E7%AF%89%E3%81%99%E3%82%8B-part1-dc34efb8ad73)
3. [Intoroduction to Apache Airflow](https://medium.com/programming-soda/apache-airflow%E3%81%A7%E3%82%A8%E3%83%B3%E3%83%89%E3%83%A6%E3%83%BC%E3%82%B6%E3%83%BC%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92%E3%83%91%E3%82%A4%E3%83%97%E3%83%A9%E3%82%A4%E3%83%B3%E3%82%92%E6%A7%8B%E7%AF%89%E3%81%99%E3%82%8B-part2-1bc31fd872c8)
4. [Develop/Test/Deploy workflow](https://medium.com/programming-soda/apache-airflow%E3%81%A7%E3%82%A8%E3%83%B3%E3%83%89%E3%83%A6%E3%83%BC%E3%82%B6%E3%83%BC%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92%E3%83%91%E3%82%A4%E3%83%97%E3%83%A9%E3%82%A4%E3%83%B3%E3%82%92%E6%A7%8B%E7%AF%89%E3%81%99%E3%82%8B-part3-c37ec8820033)
5. [Operation management and test for Airflow](https://medium.com/programming-soda/apache-airflow%E3%81%A7%E3%82%A8%E3%83%B3%E3%83%89%E3%83%A6%E3%83%BC%E3%82%B6%E3%83%BC%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92%E3%83%91%E3%82%A4%E3%83%97%E3%83%A9%E3%82%A4%E3%83%B3%E3%82%92%E6%A7%8B%E7%AF%89%E3%81%99%E3%82%8B-part4-590ad9f9fb80)
6. [From Airflow to managed service](https://medium.com/programming-soda/apache-airflow%E3%81%A7%E3%82%A8%E3%83%B3%E3%83%89%E3%83%A6%E3%83%BC%E3%82%B6%E3%83%BC%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92%E3%83%91%E3%82%A4%E3%83%97%E3%83%A9%E3%82%A4%E3%83%B3%E3%82%92%E6%A7%8B%E7%AF%89%E3%81%99%E3%82%8B-part5-end-a7ebc5e0f544)

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
dags_folder = /your_folder/airflow-ml-exercises/airflow_ml
```

Run web server.

```
airflow webserver --port 8080
```

If you want to refresh list of DAGs, execute following command.

```
python -c "from airflow.models import DagBag; d = DagBag();"
```
