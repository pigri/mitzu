from datetime import datetime

import mitzu.common.model as M
from mitzu import init_project


def test_trion_complex_data():
    target = M.EventDataSource(
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
        connection=M.Connection(
            connection_type=M.ConnectionType.TRINO,
            user_name="test",
            secret_resolver=None,
            schema="minio",
            host="localhost",
            extra_configs={"secondary_schema": "tiny"},
        ),
    )

    m = init_project(
        target, start_date=datetime(2021, 1, 1), end_date=datetime(2021, 1, 4)
    )
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
