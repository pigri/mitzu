import services.adapters.sqlite_adapter as sa
from services.discovery.dataset_discovery import EventDatasetDiscovery
from tests.samples.sources import SIMPLE_BIG_DATA
from datetime import datetime
from services.notebook.model_loader import ModelLoader
from services.common.model import Segment
import re


def assert_sql(expected: str, actual: str):
    expected = re.sub(r"\s+", "-", expected)
    actual = re.sub(r"\s+", "-", actual)

    assert expected.strip() == actual.strip()


def test_simple_big_data_discovery():
    adapter = sa.SQLiteAdapter(SIMPLE_BIG_DATA)
    discovery = EventDatasetDiscovery(
        SIMPLE_BIG_DATA, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )

    dd = discovery.discover_dataset()

    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    seg: Segment = m.app_launched

    assert_sql(
        """SELECT :param_1 AS datetime, :param_2 AS "group", 
        count(DISTINCT simple_big_data.user_id) AS unique_user_count, 
        count(simple_big_data.user_id) AS event_count 
        FROM simple_big_data WHERE simple_big_data.event_name = :event_name_1 
        GROUP BY :param_1, :param_2""",
        seg.get_sql(),
    )

    assert 30 == seg.get_df().size