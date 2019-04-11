import unittest
from unittest import mock
from airflow.models import DAG, DagBag, DagRun
from airflow.models import DagModel, errors, Pool, SlaMiss, TaskInstance
from airflow.jobs import BackfillJob, SchedulerJob, LocalTaskJob
from airflow import configuration
from airflow.utils import timezone
from airflow.utils.db import create_session


def clear_db_runs():
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagModel).delete()


def clear_db_sla_miss():
    with create_session() as session:
        session.query(SlaMiss).delete()


def clear_db_errors():
    with create_session() as session:
        session.query(errors.ImportError).delete()


def clear_db_pools():
    with create_session() as session:
        session.query(Pool).delete()


class TestDagRun(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

        def getboolean(section, key):
            if section.lower() == "core" and key.lower() == "load_examples":
                return False
            else:
                return configuration.conf.getboolean(section, key)

        cls.patcher = mock.patch("airflow.jobs.conf.getboolean")
        mock_getboolean = cls.patcher.start()
        mock_getboolean.side_effect = getboolean

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()

    def setUp(self):
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_errors()

    def test_schedule(self):
        dag = DAG(
            'test_scheduler_dagrun_once',
            start_date=timezone.datetime(2015, 1, 1),
            schedule_interval="@once")

        scheduler = SchedulerJob()
        dag.clear()
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        dr = scheduler.create_dag_run(dag)
        self.assertIsNone(dr)
