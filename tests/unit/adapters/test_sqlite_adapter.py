import mitzu.adapters.file_adapter as fa
from mitzu.model import DataType, Field
from tests.samples.sources import get_simple_big_data, get_simple_csv


def test_simple_list_columns():
    scv = get_simple_csv()
    adapter = fa.FileAdapter(scv)

    fields = adapter.list_fields(scv.event_data_tables[0])
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
    sbd = get_simple_big_data()
    adapter = fa.FileAdapter(sbd)

    fields = adapter.list_fields(sbd.event_data_tables[0])
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
