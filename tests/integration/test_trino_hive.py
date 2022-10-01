from datetime import datetime

import mitzu.model as M
from tests.helper import assert_row


def test_trino_complex_data():
    target = M.Project(
        event_data_tables=[
            M.EventDataTable.create(
                table_name="sub_events",
                event_name_alias="user_subscribe",
                event_time_field="subscription_time",
                user_id_field="subscriber_id",
            ),
            M.EventDataTable.create(
                table_name="web_events",
                event_name_field="event_name",
                event_time_field="event_time",
                user_id_field="user_id",
                event_specific_fields=["event_properties"],
            ),
        ],
        default_end_dt=datetime(2021, 1, 4),
        connection=M.Connection(
            connection_type=M.ConnectionType.TRINO,
            user_name="test",
            secret_resolver=None,
            schema="tiny",
            catalog="minio",
            host="localhost",
        ),
    )

    dp = target.discover_project()
    m = dp.create_notebook_class_model()
    pdf = (
        (m.page_visit.event_properties.url.is_not_null >> m.purchase)
        .config(
            group_by=m.page_visit.user_properties.is_subscribed,
            time_group="total",
            conv_window="1 week",
        )
        .get_df()
    )
    assert pdf is not None

    pdf = (
        (m.user_subscribe.event_properties.reason.is_referral)
        .config(group_by=m.user_subscribe.event_properties.reason, time_group="week")
        .get_df()
    )
    assert pdf is not None


def test_trino_map_types_discovery():
    target = M.Project(
        event_data_tables=[
            M.EventDataTable.create(
                table_name="sub_events",
                event_name_alias="user_subscribe",
                event_time_field="subscription_time",
                user_id_field="subscriber_id",
            ),
            M.EventDataTable.create(
                table_name="web_events",
                event_name_field="event_name",
                event_time_field="event_time",
                user_id_field="user_id",
                event_specific_fields=["event_properties"],
            ),
        ],
        default_end_dt=datetime(2021, 1, 4),
        connection=M.Connection(
            connection_type=M.ConnectionType.TRINO,
            user_name="test",
            secret_resolver=None,
            schema="tiny",
            catalog="minio",
            host="localhost",
        ),
    )

    m = target.discover_project().create_notebook_class_model()

    # Group by with Event Specific MAP type
    df = m.search.config(
        group_by=m.search.event_properties.url,
        start_dt="2021-01-01",
        end_dt="2021-02-01",
        time_group="total",
    ).get_df()

    assert_row(
        df,
        _datetime=None,
        _agg_value=2871,
        _group="www.superstore.cn",
    )

    # Group by with Event Specific MAP type
    df = m.search.config(
        group_by=m.search.event_properties.url,
        start_dt="2021-01-01",
        end_dt="2021-02-01",
        time_group="total",
        aggregation="event_count",
    ).get_df()

    assert_row(
        df,
        _datetime=None,
        _agg_value=2877,
        _group="www.superstore.cn",
    )

    # Group by with ROW type
    df = m.search.config(
        group_by=m.search.user_properties.is_subscribed,
        start_dt="2021-01-01",
        end_dt="2021-02-01",
        time_group="total",
    ).get_df()

    assert_row(
        df,
        _datetime=None,
        _agg_value=4325,
        _group=True,
    )

    # Group by with  Geneir MAP type
    df = m.user_subscribe.config(
        group_by=m.user_subscribe.event_properties.reason,
        start_dt="2021-01-01",
        end_dt="2021-02-01",
        time_group="total",
    ).get_df()

    assert_row(
        df,
        _datetime=None,
        _agg_value=114,
        _group="referral",
    )

    # Group by with properties that are present only partially present
    df = (
        (m.user_subscribe | m.add_to_cart)
        .config(
            group_by=m.add_to_cart.user_properties.is_subscribed,
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
        (m.page_visit >> m.add_to_cart)
        .config(
            group_by=m.page_visit.event_properties.url,
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
