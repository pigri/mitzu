from services.adapters.sqlite_adapter import SQLiteAdapter
from services.discovery.dataset_discovery import EventDatasetDiscovery
from tests.samples.sources import SIMPLE_BIG_DATA, SIMPLE_CSV
from datetime import datetime
from services.notebook.model_loader import ModelLoader
from services.common.model import Segment
import re
import pandas as pd  # type: ignore
from typing import List, Dict
import sqlalchemy as SA  # type: ignore


def assert_sql(expected: str, actual: str):
    expected = re.sub(r"\s+", " ", expected)
    actual = re.sub(r"\s+", " ", actual)

    assert expected.strip() == actual.strip()


def assert_row(df: pd.DataFrame, **kwargs):
    records: List[Dict] = df.to_dict("records")
    if len(records) == 0:
        assert False, f"Empty dataframe for matching {kwargs}"

    closest = {}
    closest_match = -1
    for record in records:
        match = 0
        for key, val in kwargs.items():
            if record[key] == val:
                match += 1
        if closest_match == len(kwargs) and match == closest_match:
            assert False, f"Multiple records match for {kwargs}"

        if match > closest_match:
            closest_match = match
            closest = record

    if closest_match == len(kwargs):
        assert True, f"Matching record for {kwargs}"
        return

    assert False, f"Not matching record for {kwargs}\nClosest records:\n{closest}"


def test_simple_big_data_discovery():
    adapter = SQLiteAdapter(SIMPLE_BIG_DATA)

    discovery = EventDatasetDiscovery(
        SIMPLE_BIG_DATA, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )

    dd = discovery.discover_dataset()

    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    seg: Segment = m.app_install

    print("Starting")

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
