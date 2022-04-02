from copy import copy
from typing import cast

from tests.test_samples.sources import SIMPLE_CSV, SIMPLE_BIG_DATA
from sqlalchemy import inspect  # type: ignore
from mitzu.common.model import Connection, ConnectionType, EventDataSource
from mitzu.discovery.dataset_discovery import EventDatasetDiscovery
from mitzu.adapters.adapter_factory import get_or_create_adapter
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
from mitzu.notebook.model_loader import ModelLoader
import pandas as pd  # type: ignore
import pytest
from retry import retry  # type: ignore
from datetime import datetime
from tests.helper import assert_row
from typing import Optional

from dataclasses import dataclass


@dataclass
class TestCase:
    type: ConnectionType
    url: Optional[str] = None

    def __repr__(self) -> str:
        url = "" if self.url is None else f"({self.url})"
        return f"{type}{url}"


CONFIGS = [
    TestCase(type=ConnectionType.POSTGRESQL),
    TestCase(type=ConnectionType.MYSQL),
    TestCase(type=ConnectionType.SQLITE, url="sqlite://"),
]


@retry(Exception, delay=5, tries=6)
def check_table(engine, source: EventDataSource) -> bool:
    print(f"Trying to connect to {source.table_name}")
    ins = inspect(engine)
    ret = ins.dialect.has_table(engine.connect(), source.table_name)
    return ret


def ingest_test_data(source: EventDataSource, raw_path: str) -> SQLAlchemyAdapter:
    adapter = get_or_create_adapter(source)
    adapter = cast(SQLAlchemyAdapter, adapter)
    engine = adapter.get_engine()
    ret = check_table(engine, source)

    print(f"Table {source.table_name} exists: {ret}")
    if not ret:
        if raw_path.endswith(".csv"):
            pdf = pd.read_csv(raw_path)
        elif raw_path.endswith(".parquet"):
            pdf = pd.read_parquet(raw_path)
        else:
            raise Exception("Unsupported integration test data at\n" + raw_path)
        try:
            pdf[source.event_time_field] = pdf[source.event_time_field].apply(
                lambda v: datetime.fromisoformat(v)
            )
        except Exception as exc:
            print(exc)
        pdf.to_sql(con=engine, name=source.table_name, index=False)
    return adapter


def validate_integration(adapter: SQLAlchemyAdapter, source: EventDataSource):
    discovery = EventDatasetDiscovery(
        source, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )

    dd = discovery.discover_dataset()

    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    df = m.cart.brand.is_artex.config(start_dt="2020-01-01").get_df()
    assert_row(
        df,
        _datetime=datetime(2020, 1, 1),
        _unique_user_count=1,
        _event_count=1,
        _group=None,
    )

    df = m.cart.brand.is_artex.config(start_dt="2020-01-01").get_df()
    assert_row(
        df,
        _datetime=datetime(2020, 1, 1),
        _unique_user_count=1,
        _event_count=1,
        _group=None,
    )

    df = (
        (m.view >> m.cart)
        .config(
            start_dt="2020-01-01",
            time_group="hour",
            conv_window="12 day",
            group_by=m.view.brand,
            max_group_by_count=3,
        )
        .get_df()
    )

    assert 254 == df.shape[0]
    assert_row(
        df,
        _datetime=pd.Timestamp("2020-01-01 00:00:00"),
        _unique_user_count_1=3,
        _event_count_1=7,
        _unique_user_count_2=2,
        _event_count_2=6,
        _group="cosmoprofi",
        _conversion_rate=0.66667,
    )


@pytest.mark.parametrize("config", CONFIGS)
def test_db_integrations(config: TestCase):
    real_source = SIMPLE_CSV
    raw_path = real_source.connection.extra_configs["path"]
    test_source = copy(real_source)
    test_source.connection = Connection(
        connection_type=config.type,
        url=config.url,
        user_name="test",
        password="test",
        schema="test",
        host="localhost",
    )

    adapter = ingest_test_data(source=test_source, raw_path=raw_path)
    validate_integration(adapter=adapter, source=test_source)


def test_big_data_integrations():
    real_source = SIMPLE_BIG_DATA
    raw_path = real_source.connection.extra_configs["path"]
    test_source = copy(real_source)
    test_source.connection = Connection(
        connection_type=ConnectionType.POSTGRESQL,
        user_name="test",
        password="test",
        schema="test",
        host="localhost",
    )
    adapter = ingest_test_data(source=test_source, raw_path=raw_path)
    assert adapter is not None
