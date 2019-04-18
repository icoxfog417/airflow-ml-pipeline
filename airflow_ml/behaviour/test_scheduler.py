import unittest
from datetime import datetime, timedelta, timezone
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


class TestScheduler(unittest.TestCase):

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

    def _test_schedule(self, interval, schedule_criteria, start_date=True,
                       default_start_date=-10):
        print("Test {} date of {} schedule job.".format(
            "start" if start_date else "end",
            interval
        ))
        today = datetime.today()
        today_utc = datetime.now(timezone.utc)
        _start_date = today + timedelta(days=default_start_date)
        _end_date = None
        catchup = start_date

        def format(date):
            if date is None:
                return "-"
            else:
                return date.strftime("%Y/%m/%d %H:%M:%S")

        deltas = (
            (-2, "day_before_yesterday"),
            (-1, "yesterday"),
            (0, "today"),
            (1, "tomorrow"),
            (2, "day_after_tomorrow"),
        )

        for delta, description in deltas:
            date = today + timedelta(days=delta)

            if start_date:
                _start_date = datetime.today() + timedelta(days=delta)
            else:
                _end_date = date

            dag = DAG(f"dag_starts_{description}",
                      start_date=_start_date,
                      end_date=_end_date,
                      schedule_interval=interval,
                      catchup=catchup)

            scheduler = SchedulerJob()
            dag.clear()
            dr = scheduler.create_dag_run(dag)
            execution_date = None if dr is None else dr.execution_date
            dates = (f"today={format(today)} (UTC={format(today_utc)}), ",
                     f"start_date={format(dag.start_date)}, ",
                     f"end_date={format(dag.end_date)}, ",
                     f"execution_date={format(execution_date)}")
            dates = "\n\t" + "\n\t".join(dates)

            if schedule_criteria(delta):
                self.assertIsNotNone(dr)
                print(f"{description} is scheduled {dates}.")
            else:
                self.assertIsNone(dr)
                print(f"{description} is not scheduled {dates}.")
