from services.adapters.sqlite_adapter import SQLiteAdapter
from services.discovery.dataset_discovery import EventDatasetDiscovery
from tests.samples.sources import SIMPLE_BIG_DATA, SIMPLE_CSV
from datetime import datetime
from services.notebook.model_loader import ModelLoader
from services.common.model import Segment
from tests.helper import assert_row


def test_simple_big_data_discovery():
    adapter = SQLiteAdapter(SIMPLE_BIG_DATA)

    discovery = EventDatasetDiscovery(
        SIMPLE_BIG_DATA, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )

    dd = discovery.discover_dataset()

    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    seg: Segment = m.app_install

    assert 1 == seg.get_df().shape[0]
    assert_row(seg.get_df(), unique_user_count=2254, datetime=None, event_count=4706)


def test_simple_csv_data_discovery():
    adapter = SQLiteAdapter(SIMPLE_CSV)
    discovery = EventDatasetDiscovery(
        SIMPLE_CSV, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )
    dd = discovery.discover_dataset()
    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    seg: Segment = m.cart

    assert 1 == seg.get_df().shape[0]

    assert_row(seg.get_df(), unique_user_count=108, datetime=None, event_count=787)
