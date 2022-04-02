import mitzu.adapters.file_adapter as fa
from mitzu.common.model import DataType, Field
from tests.test_samples.sources import SIMPLE_CSV, SIMPLE_BIG_DATA


def test_simple_list_columns():
    adapter = fa.FileAdapter(SIMPLE_CSV)

    fields = adapter.list_fields()
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

    fields = adapter.list_fields()
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
