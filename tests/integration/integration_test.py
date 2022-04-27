from copy import copy
from typing import Any, Dict, cast
from mitzu import init_notebook_project
from tests.test_samples.sources import SIMPLE_CSV, SIMPLE_BIG_DATA
from sqlalchemy import inspect  # type: ignore
from mitzu.common.model import (
    Connection,
    ConnectionType,
    EventDataSource,
    default_field,
)
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
import pandas as pd  # type: ignore
import pytest
from retry import retry  # type: ignore
from datetime import datetime
from tests.helper import assert_row
from dataclasses import dataclass
from sshtunnel import SSHTunnelForwarder  # type: ignore


@dataclass
class TestCase:
    type: ConnectionType
    configs: Dict = default_field(
        {
            "user_name": "test",
            "password": "test",
            "schema": "test",
            "host": "localhost",
        }
    )

    def __repr__(self) -> str:
        confs = "" if self.configs is None else f"({self.configs})"
        return f"{type}{confs}"


CONFIGS = [
    TestCase(type=ConnectionType.POSTGRESQL),
    TestCase(type=ConnectionType.MYSQL),
    TestCase(type=ConnectionType.SQLITE, configs={"url": "sqlite://"}),
]


@retry(Exception, delay=5, tries=6)
def check_table(engine, source: EventDataSource) -> bool:
    ed_table = source.event_data_tables[0]
    print(f"Trying to connect to {ed_table.table_name}")
    ins = inspect(engine)
    return ins.dialect.has_table(
        engine.connect(), source.event_data_tables[0].table_name
    )


def ingest_test_data(source: EventDataSource, raw_path: str) -> SQLAlchemyAdapter:
    adapter = cast(SQLAlchemyAdapter, source.adapter)
    engine = adapter.get_engine()
    ret = check_table(engine, source)
    ed_table = source.event_data_tables[0]

    print(f"Table {ed_table.table_name} exists: {ret}")
    if not ret:
        if raw_path.endswith(".csv"):
            pdf = pd.read_csv(raw_path)
        elif raw_path.endswith(".parquet"):
            pdf = pd.read_parquet(raw_path)
        else:
            raise Exception("Unsupported integration test data at\n" + raw_path)
        try:
            pdf[ed_table.event_time_field] = pdf[ed_table.event_time_field].apply(
                lambda v: datetime.fromisoformat(v)
            )
        except Exception as exc:
            print(exc)
        pdf.to_sql(con=engine, name=ed_table.table_name, index=False)
    return adapter


def validate_integration(source: EventDataSource):
    m = cast(
        Any,
        init_notebook_project(
            source=source, start_dt=datetime(2021, 1, 1), end_dt=datetime(2022, 1, 1)
        ),
    )

    df = m.cart.brand.is_artex.config(start_dt="2020-01-01").get_df()
    assert_row(
        df,
        _datetime=datetime(2020, 1, 1),
        _unique_user_count=1,
        _event_count=1,
        _group=None,
    )

    df = m.cart.brand.is_artex.config(start_dt="2020-01-01").get_df()
    assert_row(
        df,
        _datetime=datetime(2020, 1, 1),
        _unique_user_count=1,
        _event_count=1,
        _group=None,
    )

    df = (
        (m.view >> m.cart)
        .config(
            start_dt="2020-01-01",
            time_group="hour",
            conv_window="12 day",
            group_by=m.view.brand,
            max_group_by_count=3,
        )
        .get_df()
    )

    assert 254 == df.shape[0]
    assert_row(
        df,
        _datetime=pd.Timestamp("2020-01-01 00:00:00"),
        _unique_user_count_1=3,
        _event_count_1=7,
        _unique_user_count_2=2,
        _event_count_2=6,
        _group="cosmoprofi",
        _conversion_rate=0.66667,
    )

    df = (
        (m.view >> m.cart)
        .config(
            start_dt="2020-01-01",
            time_group="week",
            conv_window="12 day",
            group_by=m.view.brand,
            max_group_by_count=3,
        )
        .get_df()
    )

    assert 99 == df.shape[0]
    assert_row(
        df,
        _datetime=pd.Timestamp("2019-12-30 00:00:00"),
        _unique_user_count_1=6,
        _event_count_1=12,
        _unique_user_count_2=2,
        _event_count_2=6,
        _group="cosmoprofi",
        _conversion_rate=0.33333,
    )


@pytest.mark.parametrize("config", CONFIGS)
def test_db_integrations(config: TestCase):
    test_source = copy(SIMPLE_CSV)
    raw_path = test_source.connection.extra_configs["path"]
    test_source.connection = Connection(connection_type=config.type, **config.configs)

    ingest_test_data(source=test_source, raw_path=raw_path)
    validate_integration(source=test_source)


def test_ssh_tunnel():
    server = SSHTunnelForwarder(
        ssh_address_or_host=("localhost", 2222),
        ssh_username="test",
        ssh_password="test",
        remote_bind_address=("postgres_tunnel", 5432),
        local_bind_address=("0.0.0.0", 5433),
    )

    test_source = copy(SIMPLE_CSV)
    raw_path = test_source.connection.extra_configs["path"]
    test_source.connection = Connection(
        connection_type=ConnectionType.POSTGRESQL,
        user_name="test",
        password="test",
        schema="test",
        port=5433,
        host="localhost",
        ssh_tunnel_forwarder=server,
    )
    server.close()

    ingest_test_data(source=test_source, raw_path=raw_path)
    validate_integration(source=test_source)


@pytest.mark.skip
def test_big_data_integarion():
    real_source = SIMPLE_BIG_DATA
    raw_path = real_source.connection.extra_configs["path"]
    test_source = copy(real_source)
    test_source.connection = Connection(
        connection_type=ConnectionType.POSTGRESQL,
        user_name="test",
        password="test",
        schema="test",
        host="localhost",
    )

    ingest_test_data(source=test_source, raw_path=raw_path)
