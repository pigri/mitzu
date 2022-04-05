from mitzu.common.model import Connection, ConnectionType, EventDataSource
import os

WD = os.path.dirname(os.path.abspath(__file__))

SIMPLE_CSV = EventDataSource(
    table_name="simple_dataset",
    event_name_field="event_type",
    user_id_field="user_id",
    event_time_field="event_time",
    # event_specific_fields=["brand"],
    max_enum_cardinality=300,
    max_map_key_cardinality=300,
    connection=Connection(
        connection_type=ConnectionType.FILE,
        extra_configs={
            "file_type": "csv",
            "path": WD + "/simple.csv",
        },
    ),
)

SIMPLE_BIG_DATA = EventDataSource(
    table_name="simple_big_data",
    event_name_field="event_name",
    user_id_field="user_id",
    event_time_field="event_time",
    max_enum_cardinality=300,
    max_map_key_cardinality=300,
    connection=Connection(
        connection_type=ConnectionType.FILE,
        extra_configs={
            "file_type": "parquet",
            "path": WD + "/simple_big_data.snappy.parquet",
        },
    ),
)

ECOMMERCE_EVENTS = EventDataSource(
    table_name="ecommerce_events",
    event_name_field="event_name",
    user_id_field="user_id",
    event_time_field="event_time",
    max_enum_cardinality=300,
    max_map_key_cardinality=300,
    event_specific_fields=["element_id"],
    connection=Connection(
        connection_type=ConnectionType.FILE,
        extra_configs={
            "file_type": "parquet",
            "path": WD + "/ecommerce_events.parquet",
        },
    ),
)
