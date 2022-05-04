import mitzu.adapters.file_adapter as fa
from mitzu.common.model import DataType, Field
from tests.test_samples.sources import SIMPLE_CSV, SIMPLE_BIG_DATA


def test_simple_list_columns():
    adapter = fa.FileAdapter(SIMPLE_CSV)

    fields = adapter.list_fields(SIMPLE_CSV.event_data_tables[0])
    assert 9 == len(fields)
    assert 3 == len(
        set(fields)
        & set(
            [
                Field("user_id", DataType.NUMBER),
                Field("event_time", DataType.DATETIME),
                Field("event_type", DataType.STRING),
            ]
        )
    )


def test_complex_list_columns():
    adapter = fa.FileAdapter(SIMPLE_BIG_DATA)

    fields = adapter.list_fields(SIMPLE_BIG_DATA.event_data_tables[0])
    assert 3 == len(
        set(fields)
        & set(
            [
                Field("user_id", DataType.STRING),
                Field("event_time", DataType.DATETIME),
                Field("event_name", DataType.STRING),
            ]
        )
    )
