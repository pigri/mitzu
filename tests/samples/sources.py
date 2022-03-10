from services.common.model import Connection, ConnectionType, EventDataSource
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
