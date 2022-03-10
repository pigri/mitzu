import services.adapters.sqlite_adapter as fa
from services.common.model import DataType, Field
from tests.samples.sources import SIMPLE_CSV


def test_simple_list_columns():
    adapter = fa.SQLiteAdapter(SIMPLE_CSV)

    fields = adapter.list_fields()
    print(fields)
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
