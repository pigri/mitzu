import services.adapters.sqlite_adapter as fa
from services.common.model import DataType, Field
from tests.samples.sources import SIMPLE_CSV, COMPLEX_PARQUET


def test_simple_list_columns():
    adapter = fa.SQLiteAdapter(SIMPLE_CSV)

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
    adapter = fa.SQLiteAdapter(COMPLEX_PARQUET)

    fields = adapter.list_fields()
    print([f"{f.name}: {f.type}" for f in fields])
    assert 5 == len(
        set(fields)
        & set(
            [
                Field("unified_user_id", DataType.STRING),
                Field("event_time", DataType.DATETIME),
                Field("event_name", DataType.STRING),
                Field("event_properties", DataType.STRING),
                Field("user_details", DataType.STRING),
            ]
        )
    )
    