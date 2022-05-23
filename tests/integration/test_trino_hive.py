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
        target, start_date=datetime(2021, 1, 1), end_date=datetime(2022, 1, 1)
    )

    pdf = (m.user_subscribe.event_properties.reason.is_referral).get_df()

    pdf = (m.page_visit).config(group_by=m.page_visit.event_name).get_df()
    assert pdf is not None
