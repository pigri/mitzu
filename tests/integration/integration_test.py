from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

import mitzu.model as M
import pandas as pd
import pytest
from tests.helper import assert_row, ingest_test_file_data
from tests.samples.sources import get_simple_csv

WD = os.path.dirname(os.path.abspath(__file__)) + "/../samples/"


def case_name(tc: TestCase):
    return f"{tc.connection.connection_type.name} {tc.connection.schema}"


@dataclass(frozen=True)
class TestCase:
    connection: M.Connection
    ingest: bool = True


def def_con(type: M.ConnectionType) -> M.Connection:
    return M.Connection(
        connection_type=type,
        host="localhost",
        secret_resolver=M.ConstSecretResolver("test"),
        user_name="test",
    )


TEST_CASES = [
    TestCase(
        M.Connection(
            connection_type=M.ConnectionType.MYSQL,
            host="localhost",
            secret_resolver=M.ConstSecretResolver("test"),
            user_name="test",
            port=3307,
            schema="test",
        ),
    ),
    TestCase(def_con(M.ConnectionType.POSTGRESQL)),
    TestCase(
        M.Connection(
            connection_type=M.ConnectionType.TRINO,
            host="localhost",
            secret_resolver=None,
            user_name="test",
            port=8080,
            schema="test",
            catalog="mysql",
        ),
        ingest=False,
    ),
    TestCase(
        M.Connection(
            connection_type=M.ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD,
            },
        ),
        ingest=False,
    ),
]


@pytest.mark.parametrize("test_case", TEST_CASES, ids=case_name)
def test_db_integrations(test_case: TestCase):
    test_source = get_simple_csv()
    ingested_source = M.Project(
        test_case.connection,
        event_data_tables=test_source.event_data_tables,
        default_end_dt=datetime(2022, 1, 1),
        default_discovery_lookback_days=2000,
    )

    if test_case.ingest:
        ingest_test_file_data(
            project=test_source,
            target_connection=ingested_source.connection,
            transform_dt_col=False,
        )
    validate_integration(project=ingested_source)


def validate_integration(project: M.Project):
    m = project.discover_project().create_notebook_class_model()

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

    df = (
        (m.view >> m.cart)
        .config(
            start_dt="2020-01-01",
            time_group="week",
            conv_window="1 week",
            group_by=m.view.brand,
            max_group_by_count=1,
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
