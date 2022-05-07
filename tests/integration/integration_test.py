from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, cast

import mitzu.common.model as M
import pandas as pd
import pytest
from mitzu import init_notebook_project
from tests.helper import assert_row
from tests.integration.helper import ingest_test_data
from tests.test_samples.sources import get_simple_csv


@dataclass(frozen=True)
class TestCase:
    connection_type: M.ConnectionType
    connection_configs: Dict = M.default_field(
        {
            "user_name": "test",
            "secret_resolver": M.ConstSecretResolver("test"),
            "schema": "test",
            "host": "localhost",
        }
    )
    table_name_override: Optional[str] = None
    ingest: bool = True

    def __repr__(self) -> str:
        confs = (
            "" if self.connection_configs is None else f"({self.connection_configs})"
        )
        return f"{type}{confs}"


TEST_CASES = [
    TestCase(connection_type=M.ConnectionType.POSTGRESQL),
    TestCase(connection_type=M.ConnectionType.MYSQL),
    TestCase(
        connection_type=M.ConnectionType.TRINO,
        connection_configs={
            "host": "localhost",
            "secret_resolver": None,
            "user_name": "test",
            "port": 8080,
            "schema": "mysql",
            "extra_configs": {"secondary_schema": "test"},
        },
        ingest=False,
        table_name_override="test.simple_dataset",
    ),
    TestCase(
        connection_type=M.ConnectionType.SQLITE, connection_configs={"url": "sqlite://"}
    ),
]


@pytest.mark.parametrize("test_case", TEST_CASES)
def test_db_integrations(test_case: TestCase):
    test_source = get_simple_csv()
    raw_path = test_source.connection.extra_configs["path"]
    real_source = copy_source(
        test_source,
        test_case,
    )

    if test_case.ingest:
        ingest_test_data(source=real_source, raw_path=raw_path)
    validate_integration(source=real_source)


def copy_source(test_source: M.EventDataSource, test_case: TestCase):
    edt_vals = test_source.event_data_tables[0].__dict__
    if test_case.table_name_override is not None:
        edt_vals["table_name"] = test_case.table_name_override

    return M.EventDataSource(
        event_data_tables=[M.EventDataTable(**edt_vals)],
        connection=M.Connection(
            connection_type=test_case.connection_type, **test_case.connection_configs
        ),
    )


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
