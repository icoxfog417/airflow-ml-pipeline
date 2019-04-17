import unittest
from datetime import datetime, timedelta
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

    def test_daily_schedule_start(self):
        self._test_schedule("@daily", lambda delta: delta < -1)

    def test_hourly_schedule_start(self):
        self._test_schedule("@hourly", lambda delta: delta < 0)

    def test_daily_schedule_end(self):
        self._test_schedule("@daily", lambda delta: delta > -2, False)

    def test_hourly_schedule_end(self):
        self._test_schedule("@hourly", lambda delta: delta > -1, False)

    def _test_schedule(self, interval, schedule_criteria, start_date=True):
        today = datetime.today()

        for delta in (-2, -1, 0, 1, 2):
            date = today + timedelta(days=delta)
            description = "today"
            if delta == -2:
                description = "day_before_yesterday"
            elif delta == -1:
                description = "yesterday"
            elif delta == 1:
                description = "tomorrow"
            elif delta == 2:
                description = "day_after_tomorrow"

            if start_date:
                dag = DAG(f"dag_starts_{description}",
                          start_date=date,
                          schedule_interval=interval,
                          catchup=False)
            else:
                dag = DAG(f"dag_starts_{description}",
                          start_date=(today + timedelta(days=-10)),
                          end_date=date,
                          schedule_interval=interval,
                          catchup=False)

            scheduler = SchedulerJob()
            dag.clear()
            dr = scheduler.create_dag_run(dag)
            execution_date = "-" if dr is None else dr.execution_date.strftime("%Y/%m/%d %H:%M:%S")
            end_date = "-" if dag.end_date is None else dag.end_date.strftime("%Y/%m/%d %H:%M:%S")

            date_description = (f'today={today.strftime("%Y/%m/%d %H:%M:%S")}, ',
                                f'start_date={dag.start_date.strftime("%Y/%m/%d %H:%M:%S")}, ',
                                f'end_date={end_date}, ',
                                f'execution_date={execution_date}')

            if schedule_criteria(delta):
                self.assertIsNotNone(dr)
                print(f"{description} is scheduled ({date_description}).")
            else:
                self.assertIsNone(dr)
                print(f"{description} is not scheduled ({date_description}).")
