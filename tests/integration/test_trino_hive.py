from datetime import datetime

import mitzu.model as M

from tests.helper import assert_row
import pytest


@pytest.mark.skip
def test_trino_complex_data():
    target = M.Project(
        project_name="trino_project",
        event_data_tables=[
            M.EventDataTable.create(
                table_name="sub_events",
                schema="tiny",
                event_name_alias="user_subscribe",
                event_time_field="subscription_time",
                user_id_field="subscriber_id",
            ),
            M.EventDataTable.create(
                table_name="web_events",
                schema="tiny",
                event_name_field="event_name",
                event_time_field="event_time",
                user_id_field="user_id",
                event_specific_fields=[
                    "event_properties",
                    "event_name",
                    "user_properties",
                ],
            ),
        ],
        discovery_settings=M.DiscoverySettings(
            end_dt=datetime(2021, 1, 4),
        ),
        webapp_settings=M.WebappSettings(
            end_date_config=M.WebappEndDateConfig.CUSTOM_DATE,
            custom_end_date=datetime(2021, 1, 4),
        ),
        connection=M.Connection(
            connection_name="trino_connection",
            connection_type=M.ConnectionType.TRINO,
            user_name="test",
            secret_resolver=None,
            catalog="minio",
            host="localhost",
        ),
    )

    dp = target.discover_project()
    m = dp.create_notebook_class_model()
    pdf = (
        (
            m.page_visit.event_properties.url.is_not_null.group_by(
                m.page_visit.user_properties.is_subscribed
            )
            >> m.purchase
        )
        .config(
            time_group="total",
            conv_window="1 week",
        )
        .get_df()
    )
    assert pdf is not None

    pdf = (
        (m.user_subscribe.event_properties.reason.is_referral)
        .group_by(m.user_subscribe.event_properties.reason)
        .config(time_group="week")
        .get_df()
    )
    assert pdf is not None


@pytest.mark.skip
def test_trino_map_types_discovery():
    target = M.Project(
        project_name="trino_project",
        event_data_tables=[
            M.EventDataTable.create(
                table_name="sub_events",
                schema="tiny",
                event_name_alias="user_subscribe",
                event_time_field="subscription_time",
                user_id_field="subscriber_id",
                event_specific_fields=[
                    "event_properties",
                    "subscriber_id",
                    "subscription_time",
                ],
            ),
            M.EventDataTable.create(
                table_name="web_events",
                schema="tiny",
                event_name_field="event_name",
                event_time_field="event_time",
                user_id_field="user_id",
                event_specific_fields=[
                    "event_properties",
                    "event_name",
                    "user_properties",
                ],
            ),
        ],
        discovery_settings=M.DiscoverySettings(
            end_dt=datetime(2021, 1, 4),
        ),
        webapp_settings=M.WebappSettings(
            end_date_config=M.WebappEndDateConfig.CUSTOM_DATE,
            custom_end_date=datetime(2021, 1, 4),
        ),
        connection=M.Connection(
            connection_name="trino_project",
            connection_type=M.ConnectionType.TRINO,
            user_name="test",
            secret_resolver=None,
            catalog="minio",
            host="localhost",
        ),
    )

    m = target.discover_project().create_notebook_class_model()

    # Group by with Event Specific MAP type
    df = (
        m.search.group_by(m.search.event_properties.url)
        .config(
            start_dt="2021-01-01",
            end_dt="2021-02-01",
            time_group="total",
        )
        .get_df()
    )

    assert_row(
        df,
        _datetime=None,
        _agg_value=2871,
        _group="www.superstore.cn",
    )

    # Group by with Event Specific MAP type
    df = (
        m.search.group_by(m.search.event_properties.url)
        .config(
            start_dt="2021-01-01",
            end_dt="2021-02-01",
            time_group="total",
            aggregation="event_count",
        )
        .get_df()
    )

    assert_row(
        df,
        _datetime=None,
        _agg_value=2877,
        _group="www.superstore.cn",
    )

    # Group by with ROW type
    df = (
        m.search.group_by(m.search.user_properties.is_subscribed)
        .config(
            start_dt="2021-01-01",
            end_dt="2021-02-01",
            time_group="total",
        )
        .get_df()
    )

    assert_row(
        df,
        _datetime=None,
        _agg_value=4325,
        _group=True,
    )

    # Group by with  Geneir MAP type
    df = (
        m.user_subscribe.group_by(m.user_subscribe.event_properties.reason)
        .config(
            start_dt="2021-01-01",
            end_dt="2021-02-01",
            time_group="total",
        )
        .get_df()
    )

    assert_row(
        df,
        _datetime=None,
        _agg_value=114,
        _group="referral",
    )

    # Group by with properties that are present only partially present
    df = (
        (m.user_subscribe | m.add_to_cart)
        .group_by(m.add_to_cart.user_properties.is_subscribed)
        .config(
            start_dt="2021-01-01",
            end_dt="2021-02-01",
            time_group="total",
        )
        .get_df()
    )

    assert_row(
        df,
        _datetime=None,
        _agg_value=4292,
        _group=False,
    )

    assert_row(
        df,
        _datetime=None,
        _agg_value=417,
        _group=None,
    )

    # Median conversion type
    df = (
        (m.page_visit.group_by(m.page_visit.event_properties.url) >> m.add_to_cart)
        .config(
            start_dt="2021-01-01",
            end_dt="2021-02-01",
            time_group="total",
            conv_window="7 days",
            aggregation="ttc_median",
        )
        .get_df()
    )
    assert_row(
        df,
        _datetime=None,
        _agg_value_1=0,
        _agg_value_2=356499,
        _group="www.awestore.com",
    )
    assert_row(
        df,
        _datetime=None,
        _agg_value_1=0,
        _agg_value_2=224622,
        _group="www.superstore.cn",
    )
