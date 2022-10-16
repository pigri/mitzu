import os
import pytest
from unittest.mock import MagicMock

from mitzu.model import (
    Connection,
    ConnectionType,
    EventDataTable,
    Project,
    InvalidProjectError,
    InvalidEventDataTableError,
    Field,
    DataType,
)

WD = os.path.dirname(os.path.abspath(__file__))


def test_project_without_event_data_table():
    project = Project(
        event_data_tables=[],
        connection=Connection(
            connection_type=ConnectionType.FILE,
            extra_configs={
                "file_type": "csv",
                "path": WD + "/",
            },
        ),
    )

    with pytest.raises(InvalidProjectError):
        project.validate()


def test_event_data_table_having_event_name_alias_and_field():
    adapter = MagicMock()

    edt = EventDataTable.create(
        table_name="simple",
        event_name_alias="event_type",
        event_name_field="event_type",
        user_id_field="user_id",
        event_time_field="event_time",
        date_partition_field="event_time",
    )

    with pytest.raises(InvalidEventDataTableError) as error:
        edt.validate(adapter)

    assert (
        "both event_name_alias and event_name_field can't be defined in the same time"
        in str(error.value)
    )


def test_event_data_table_without_event_name_alias_and_field():
    adapter = MagicMock()
    edt = EventDataTable.create(
        table_name="simple",
        user_id_field="user_id",
        event_time_field="event_time",
        date_partition_field="event_time",
    )

    with pytest.raises(InvalidEventDataTableError) as error:
        edt.validate(adapter)

    assert "define the event_name_alias or the event_name_field" in str(error.value)


def test_event_data_table_without_any_fields():
    adapter = MagicMock()
    edt = EventDataTable.create(
        table_name="simple",
        event_name_field="event_type",
        user_id_field="user_id",
        event_time_field="event_time",
        date_partition_field="event_time",
    )

    with pytest.raises(InvalidEventDataTableError) as error:
        edt.validate(adapter)

    assert "No fields in event data table 'simple'" in str(error.value)


FIELD_NAMES_TO_VALIDATE = [
    "event_name_field",
    "user_id_field",
    "event_time_field",
    "date_partition_field",
]


@pytest.mark.parametrize("missing_field_name", FIELD_NAMES_TO_VALIDATE)
def test_event_data_table_with_missing_field(missing_field_name: str):
    fields = {
        "event_name_field": "event_type",
        "user_id_field": "user_id",
        "event_time_field": "event_time",
        "date_partition_field": "event_time",
    }
    fields[missing_field_name] = "missing"
    adapter = MagicMock()
    adapter.list_fields.return_value = [
        Field("event_type", DataType.STRING),
        Field("user_id", DataType.STRING),
        Field("event_time", DataType.DATETIME),
    ]
    edt = EventDataTable.create(table_name="simple", **fields)  # type: ignore

    with pytest.raises(InvalidEventDataTableError) as error:
        edt.validate(adapter)

    assert "does not have 'missing'" in str(error.value)
