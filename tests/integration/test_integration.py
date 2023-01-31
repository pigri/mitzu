from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

import mitzu.model as M
import pandas as pd
import pytest
from tests.helper import assert_row, ingest_test_file_data
from tests.samples.sources import (
    get_simple_csv,
    get_basic_events_csv,
)

WD = os.path.dirname(os.path.abspath(__file__)) + "/../samples/"


def case_name(tc: IntegrationTestCase):
    return f"{tc.connection.connection_type.name} {tc.connection.schema}"


@dataclass(frozen=True)
class IntegrationTestCase:
    connection: M.Connection
    schema: str
    ingest: bool = True


TEST_CASES = [
    IntegrationTestCase(
        M.Connection(
            connection_name="Sample project",
            connection_type=M.ConnectionType.MYSQL,
            host="localhost",
            secret_resolver=M.ConstSecretResolver("test"),
            user_name="test",
            port=3307,
        ),
        schema="test",
    ),
    IntegrationTestCase(
        M.Connection(
            connection_name="Sample project",
            connection_type=M.ConnectionType.POSTGRESQL,
            host="localhost",
            secret_resolver=M.ConstSecretResolver("test"),
            user_name="test",
        ),
        schema="public",
    ),
    IntegrationTestCase(
        M.Connection(
            connection_name="Sample project",
            connection_type=M.ConnectionType.TRINO,
            host="localhost",
            secret_resolver=None,
            user_name="test",
            port=8080,
            catalog="mysql",
        ),
        ingest=False,
        schema="test",
    ),
    IntegrationTestCase(
        M.Connection(
            connection_name="Sample project",
            connection_type=M.ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD,
            },
        ),
        ingest=False,
        schema="main",
    ),
]


@pytest.mark.parametrize("test_case", TEST_CASES, ids=case_name)
def test_db_integrations(test_case: IntegrationTestCase):
    test_source = get_simple_csv()
    test_subscriptions = get_basic_events_csv()
    target_edts = [
        M.EventDataTable(
            table_name=edt.table_name,
            schema=test_case.schema,
            user_id_field=edt.user_id_field,
            event_time_field=edt.event_time_field,
            event_name_field=edt.event_name_field,
            event_name_alias=edt.event_name_alias,
            ignored_fields=edt.ignored_fields,
            event_specific_fields=edt.event_specific_fields,
            date_partition_field=edt.date_partition_field,
        )
        for edt in (
            test_source.event_data_tables + test_subscriptions.event_data_tables
        )
    ]

    ingested_source = M.Project(
        project_name="integration_test",
        connection=test_case.connection,
        event_data_tables=target_edts,
        discovery_settings=M.DiscoverySettings(
            end_dt=datetime(2022, 1, 1),
            lookback_days=2000,
        ),
    )

    if test_case.ingest:
        ingest_test_file_data(
            source_project=test_source,
            target_connection=ingested_source.connection,
            transform_dt_col=False,
            schema=test_case.schema,
        )
        ingest_test_file_data(
            source_project=test_subscriptions,
            schema=test_case.schema,
            target_connection=ingested_source.connection,
            transform_dt_col=False,
        )
    validate_segmentation(project=ingested_source)
    validate_funnel(project=ingested_source)
    validate_retention(project=ingested_source)


def validate_segmentation(project: M.Project):
    m = project.discover_project().create_notebook_class_model()

    df = m.cart.brand.is_artex.config(
        start_dt="2020-01-01", aggregation="event_count"
    ).get_df()
    assert 1 == df.shape[0]
    assert_row(
        df,
        _datetime=datetime(2020, 1, 1),
        _agg_value=1,
        _group=None,
    )


def validate_funnel(project: M.Project):
    m = project.discover_project().create_notebook_class_model()
    df = (
        (m.view >> m.cart)
        .config(
            start_dt="2020-01-01",
            time_group="hour",
            conv_window="12 day",
            group_by=m.view.brand,
            max_group_by_count=3,
            aggregation="conversion",
        )
        .get_df()
    )

    assert 254 == df.shape[0]
    assert_row(
        df,
        _datetime=pd.Timestamp("2020-01-01 00:00:00"),
        _user_count_1=3,
        _agg_value_1=100.0,
        _user_count_2=2,
        _agg_value_2=66.6667,
        _group="cosmoprofi",
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
        _user_count_1=6,
        _agg_value_1=100.0,
        _user_count_2=2,
        _agg_value_2=33.3333,
        _group="cosmoprofi",
    )

    df = (
        (m.view >> m.cart)
        .config(
            start_dt="2020-01-01",
            time_group="week",
            conv_window="12 day",
            group_by=m.view.brand,
            max_group_by_count=3,
            aggregation="ttc_avg",
        )
        .get_df()
    )

    assert 99 == df.shape[0]
    assert_row(
        df,
        _datetime=pd.Timestamp("2019-12-30 00:00:00"),
        _user_count_1=6,
        _agg_value_1=0,
        _user_count_2=2,
        _agg_value_2=172.666,
        _group="cosmoprofi",
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
        _user_count_1=6,
        _agg_value_1=100.0,
        _user_count_2=2,
        _agg_value_2=33.3333,
        _group="cosmoprofi",
    )


def validate_retention(project: M.Project):
    m = project.discover_project().create_notebook_class_model()
    metric = (m.event_1 >= m.event_2).config(
        start_dt="2021-01-01",
        end_dt="2021-01-06",
        retention_window="1 day",
        time_group="total",
    )
    retention_df = metric.get_df()
    retention_df = retention_df.round({"_agg_value": 1})
    retention_df = retention_df.sort_values(by=["_datetime", "_ret_index"]).reset_index(
        drop=True
    )

    expected_retention_df = pd.DataFrame(
        {
            "_datetime": [None for _ in range(0, 6)],
            "_group": [None for _ in range(0, 6)],
            "_ret_index": [i for i in range(0, 6)],
            "_user_count_1": [3 for i in range(0, 6)],
            "_user_count_2": [3, 2, 1, 1, 1, 0],
            "_agg_value": [100.0, 66.7, 33.3, 33.3, 33.3, 0],
        }
    )

    pd.testing.assert_frame_equal(retention_df, expected_retention_df)

    metric = (m.new_subscription >= m.renewed_subscription).config(
        start_dt="2021-01-01",
        end_dt="2021-06-01",
        retention_window="1 month",
        time_group="total",
    )

    retention_df = metric.get_df()
    retention_df = retention_df.round({"_agg_value": 1})
    retention_df = retention_df.sort_values(by=["_datetime", "_ret_index"]).reset_index(
        drop=True
    )

    expected_retention_df = pd.DataFrame(
        {
            "_datetime": [None for _ in range(0, 6)],
            "_group": [None for _ in range(0, 6)],
            "_ret_index": [i for i in range(0, 6)],
            "_user_count_1": [1000 for i in range(0, 6)],
            "_user_count_2": [
                919,
                878,
                707,
                606,
                505,
                404,
            ],
            "_agg_value": [
                91.9,
                87.8,
                70.7,
                60.6,
                50.5,
                40.4,
            ],
        }
    )

    pd.testing.assert_frame_equal(retention_df, expected_retention_df)

    metric = (m.renewed_subscription >= m.renewed_subscription).config(
        start_dt="2021-01-01",
        end_dt="2021-02-14",
        retention_window="5 week",
        time_group="month",
    )
    retention_df = metric.get_df()

    retention_df = retention_df.round({"_agg_value": 1})
    retention_df = retention_df.sort_values(by=["_datetime", "_ret_index"]).reset_index(
        drop=True
    )

    dt_1 = datetime(2021, 1, 1)
    dt_2 = datetime(2021, 2, 1)

    expected_retention_df = pd.DataFrame(
        {
            "_datetime": [dt_1, dt_1, dt_2, dt_2],
            "_group": [None for _ in range(0, 4)],
            "_ret_index": [0, 5, 0, 5],
            "_user_count_1": [919, 919, 878, 878],
            "_user_count_2": [
                878,
                707,
                707,
                0,
            ],  # The last retention period is out of the prefiltered data
            "_agg_value": [95.5, 76.9, 80.5, 0.0],
        }
    )

    pd.testing.assert_frame_equal(retention_df, expected_retention_df)
