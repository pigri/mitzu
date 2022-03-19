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
        connection_params={"file_type": "csv"},
        url=WD + "/simple.csv",
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
        connection_params={"file_type": "parquet"},
        url=WD + "/simple_big_data.snappy.parquet",
    ),
)

COMPLEX_PARQUET = EventDataSource(
    table_name="user_test_data",
    event_name_field="event_name",
    user_id_field="unified_user_id",
    event_time_field="event_time",
    event_specific_fields=["event_properties"],
    max_enum_cardinality=300,
    max_map_key_cardinality=300,
    connection=Connection(
        connection_type=ConnectionType.FILE,
        connection_params={"file_type": "parquet"},
        url=WD + "/user_test_data.snappy.parquet",
    ),
)