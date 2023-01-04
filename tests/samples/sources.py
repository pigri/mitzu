import os
from datetime import datetime

from mitzu.model import (
    Connection,
    ConnectionType,
    DiscoverySettings,
    EventDataTable,
    Project,
)

WD = os.path.dirname(os.path.abspath(__file__))


def get_simple_csv() -> Project:
    return Project(
        project_name="simple_project",
        event_data_tables=[
            EventDataTable.create(
                table_name="simple",
                schema="main",
                event_name_field="event_type",
                user_id_field="user_id",
                event_time_field="event_time",
                date_partition_field="event_time",
            )
        ],
        discovery_settings=DiscoverySettings(
            max_enum_cardinality=300,
            max_map_key_cardinality=300,
            end_dt=datetime(2022, 1, 1),
            lookback_days=2000,
        ),
        connection=Connection(
            connection_name="Sample project",
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD + "/",
            },
        ),
    )


def get_simple_big_data() -> Project:
    return Project(
        project_name="simple_big_project",
        event_data_tables=[
            EventDataTable.create(
                table_name="simple_big_data",
                schema="main",
                event_name_field="event_name",
                user_id_field="user_id",
                event_time_field="event_time",
            )
        ],
        discovery_settings=DiscoverySettings(
            max_enum_cardinality=300,
            max_map_key_cardinality=300,
            end_dt=datetime(2022, 1, 1),
            lookback_days=2000,
            property_sample_rate=100,
            min_property_sample_size=10,
        ),
        connection=Connection(
            connection_name="Sample project",
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "parquet",
                "path": WD + "/",
            },
        ),
    )


def get_project_without_records() -> Project:
    return Project(
        project_name="simple_project",
        event_data_tables=[
            EventDataTable.create(
                table_name="simple",
                schema="main",
                event_name_field="event_type",
                user_id_field="user_id",
                event_time_field="event_time",
                date_partition_field="event_time",
            )
        ],
        discovery_settings=DiscoverySettings(
            max_enum_cardinality=300,
            max_map_key_cardinality=300,
        ),
        connection=Connection(
            connection_name="Sample project",
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD + "/",
            },
        ),
    )


def get_project_with_missing_table() -> Project:
    return Project(
        project_name="simple_project",
        event_data_tables=[
            EventDataTable.create(
                table_name="missing",
                event_name_field="event_type",
                user_id_field="user_id",
                event_time_field="event_time",
                date_partition_field="event_time",
            )
        ],
        discovery_settings=DiscoverySettings(
            max_enum_cardinality=300,
            max_map_key_cardinality=300,
        ),
        connection=Connection(
            connection_name="Sample project",
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD + "/",
            },
        ),
    )


def get_basic_events_csv() -> Project:
    return Project(
        project_name="simple_big_project",
        event_data_tables=[
            EventDataTable.create(
                table_name="subscriptions",
                event_name_field="event_type",
                user_id_field="user_id",
                event_time_field="event_time",
            ),
            EventDataTable.create(
                table_name="basic_events",
                event_name_field="event_type",
                user_id_field="user_id",
                event_time_field="event_time",
            ),
        ],
        discovery_settings=DiscoverySettings(
            max_enum_cardinality=300,
            max_map_key_cardinality=300,
            end_dt=datetime(2022, 1, 1),
            lookback_days=2000,
        ),
        connection=Connection(
            connection_name="Sample project",
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD + "/",
            },
        ),
    )
