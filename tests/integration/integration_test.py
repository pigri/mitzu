from __future__ import annotations
from typing import Any, Dict, cast, Optional
from mitzu import init_notebook_project
from tests.test_samples.sources import get_simple_csv
from tests.integration.helper import ingest_test_data
import mitzu.common.model as M
import pandas as pd
import pytest
from datetime import datetime
from tests.helper import assert_row
from dataclasses import dataclass


@dataclass
class TestCase:
    type: M.ConnectionType
    configs: Dict = M.default_field(
        {
            "user_name": "test",
            "password": "test",
            "schema": "test",
            "host": "localhost",
        }
    )
    ingest: bool = True
    table_name: Optional[str] = None

    def __repr__(self) -> str:
        confs = "" if self.configs is None else f"({self.configs})"
        return f"{type}{confs}"


CONFIGS = [
    TestCase(type=M.ConnectionType.POSTGRESQL),
    TestCase(type=M.ConnectionType.MYSQL),
    TestCase(
        type=M.ConnectionType.TRINO,
        configs={
            "host": "localhost",
            "password": None,
            "user_name": "test",
            "port": 8080,
            "schema": "mysql",
            "extra_configs": {"secondary_schema": "test"},
        },
        ingest=False,
        table_name="test.simple_dataset",
    ),
    TestCase(type=M.ConnectionType.SQLITE, configs={"url": "sqlite://"}),
]


@pytest.mark.parametrize("config", CONFIGS)
def test_db_integrations(config: TestCase):
    test_source = get_simple_csv()
    raw_path = test_source.connection.extra_configs["path"]
    test_source.connection = M.Connection(connection_type=config.type, **config.configs)
    if config.ingest:
        ingest_test_data(source=test_source, raw_path=raw_path)
    if config.table_name is not None:
        test_source.event_data_tables[0].table_name = config.table_name

    validate_integration(source=test_source)


def validate_integration(source: M.EventDataSource):
    m = cast(
        Any,
        init_notebook_project(
            source=source, start_dt=datetime(2021, 1, 1), end_dt=datetime(2022, 1, 1)
        ),
    )

    df = m.cart.brand.is_artex.config(start_dt="2020-01-01").get_df()
    assert 1 == df.shape[0]
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
        _conversion_rate=66.667,
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
        _conversion_rate=33.333,
    )
