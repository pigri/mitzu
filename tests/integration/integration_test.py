from copy import copy
from typing import cast

from numpy import source
from tests.test_samples.sources import SIMPLE_BIG_DATA, SIMPLE_CSV
from sqlalchemy import inspect
from mitzu.common.model import Connection, ConnectionType, EventDataSource
from mitzu.discovery.dataset_discovery import EventDatasetDiscovery
from mitzu.adapters.adapter_factory import get_or_create_adapter
from mitzu.adapters.slqalchemy_adapter import SQLAlchemyAdapter
from mitzu.notebook.model_loader import ModelLoader
import pandas as pd
import pytest
from retry import retry  # type: ignore
from datetime import datetime

CONNECTIONS = [ConnectionType.POSTGRESQL, ConnectionType.MYSQL]


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
        pdf = pd.read_csv(raw_path)
        try:
            pdf[source.event_time_field] = pdf[source.event_time_field].apply(
                lambda v: datetime.fromisoformat(v)
            )
        except Exception as exc:
            print(exc)
            pass
        pdf.to_sql(con=engine, name=source.table_name, index=False)
    return adapter


def validate_integration(adapter: SQLAlchemyAdapter, source: EventDataSource):
    discovery = EventDatasetDiscovery(
        source, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )

    dd = discovery.discover_dataset()

    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    print(m.view.config(start_dt="2020-01-01", end_dt="2021-01-01").get_df())


@pytest.mark.parametrize("con_type", CONNECTIONS)
def test_db_integrations(con_type):
    real_source = SIMPLE_CSV
    raw_path = real_source.connection.connection_params["path"]
    test_source = copy(real_source)
    test_source.connection = Connection(
        connection_type=con_type,
        connection_params={
            "user_name": "test",
            "password": "test",
            "schema": "test",
            "host": "localhost",
        },
    )

    adapter = ingest_test_data(source=test_source, raw_path=raw_path)
    validate_integration(adapter=adapter, source=test_source)
