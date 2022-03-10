from services.notebook.model_loader import ModelLoader
import services.adapters.sqlite_adapter as sa
from services.discovery.dataset_discovery import EventDatasetDiscovery
from tests.samples.sources import SIMPLE_CSV
from datetime import datetime


def test_enum():
    adapter = sa.SQLiteAdapter(SIMPLE_CSV)
    discovery = EventDatasetDiscovery(
        SIMPLE_CSV, adapter, datetime(2020, 2, 1), datetime(2020, 1, 1)
    )

    dd = discovery.discover_dataset()

    ml = ModelLoader()
    ml.create_dataset_model(dd)

    print()
