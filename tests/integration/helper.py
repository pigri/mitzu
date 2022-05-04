from typing import cast
from sqlalchemy import inspect
from mitzu.common.model import (
    EventDataSource,
)
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
import pandas as pd
from retry import retry  # type: ignore
from datetime import datetime


@retry(Exception, delay=5, tries=6)
def check_table(engine, source: EventDataSource) -> bool:
    ed_table = source.event_data_tables[0]
    print(f"Trying to connect to {ed_table.table_name}")
    ins = inspect(engine)
    return ins.dialect.has_table(
        engine.connect(), source.event_data_tables[0].table_name
    )


def ingest_test_data(source: EventDataSource, raw_path: str) -> SQLAlchemyAdapter:
    adapter = cast(SQLAlchemyAdapter, source.adapter)
    engine = adapter.get_engine()
    ret = check_table(engine, source)
    ed_table = source.event_data_tables[0]

    print(f"Table {ed_table.table_name} exists: {ret}")
    if not ret:
        if raw_path.endswith(".csv"):
            pdf = pd.read_csv(raw_path)
        elif raw_path.endswith(".parquet"):
            pdf = pd.read_parquet(raw_path)
        else:
            raise Exception("Unsupported integration test data at\n" + raw_path)
        try:
            pdf[ed_table.event_time_field] = pdf[ed_table.event_time_field].apply(
                lambda v: datetime.fromisoformat(v)
            )
        except Exception as exc:
            print(exc)
        sec_schema = source.connection.extra_configs.get("secondary_schema")
        pdf.to_sql(con=engine, name=ed_table.table_name, index=False, schema=sec_schema)
    return adapter
