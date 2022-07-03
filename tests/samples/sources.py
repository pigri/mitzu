import os
from datetime import datetime

from mitzu.model import Connection, ConnectionType, EventDataSource, EventDataTable

WD = os.path.dirname(os.path.abspath(__file__))


def get_simple_csv() -> EventDataSource:
    return EventDataSource(
        event_data_tables=[
            EventDataTable.create(
                table_name="simple",
                event_name_field="event_type",
                user_id_field="user_id",
                event_time_field="event_time",
            )
        ],
        max_enum_cardinality=300,
        max_map_key_cardinality=300,
        default_start_dt=datetime(2019, 1, 1),
        default_end_dt=datetime(2022, 1, 1),
        connection=Connection(
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD + "/",
            },
        ),
    )


def get_simple_big_data() -> EventDataSource:
    return EventDataSource(
        event_data_tables=[
            EventDataTable.create(
                table_name="simple_big_data",
                event_name_field="event_name",
                user_id_field="user_id",
                event_time_field="event_time",
            )
        ],
        max_enum_cardinality=300,
        max_map_key_cardinality=300,
        default_start_dt=datetime(2019, 1, 1),
        default_end_dt=datetime(2022, 1, 1),
        connection=Connection(
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "parquet",
                "path": WD + "/",
            },
        ),
    )


def get_generated_eds() -> EventDataSource:
    return EventDataSource(
        event_data_tables=[
            EventDataTable.create(
                table_name="web_events",
                event_name_field="event_name",
                user_id_field="user_id",
                event_time_field="event_time",
            ),
            EventDataTable.create(
                table_name="sub_events",
                event_name_alias="subscribe",
                user_id_field="subscriber_id",
                event_time_field="subscription_time",
            ),
        ],
        max_enum_cardinality=300,
        max_map_key_cardinality=300,
        connection=Connection(
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "parquet",
                "path": WD + "/generated/",
            },
        ),
    )
