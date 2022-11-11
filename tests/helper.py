import re
from datetime import datetime
from typing import Any, Dict, List, cast

import pandas as pd
import sqlalchemy as SA
from mitzu.adapters.file_adapter import FileAdapter
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
from mitzu.helper import LOGGER
from mitzu.model import Connection, EventDataTable, Project
from retry import retry  # type: ignore


def assert_sql(expected: str, actual: str):

    expected = re.sub(r"\s+", " ", expected).lower()
    actual = re.sub(r"\s+", " ", actual).lower()

    assert expected.strip() == actual.strip()


def is_similar(a, b) -> bool:
    if type(a) == float:
        return abs(a - b) < 0.1
        # Todo add more rules
    return a == b


def assert_row(df: pd.DataFrame, **kwargs):
    records: List[Dict] = df.to_dict("records")
    if len(records) == 0:
        assert False, f"Empty dataframe for matching {kwargs}"

    closest = {}
    closest_match = -1
    for record in records:
        match = 0
        for key, val in kwargs.items():
            if is_similar(record[key], val):
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


@retry(Exception, delay=5, tries=6)
def check_table(engine, ed_table: EventDataTable) -> bool:
    LOGGER.debug(f"Trying to connect to {ed_table.table_name}")
    ins = SA.inspect(engine)
    return ins.dialect.has_table(engine.connect(), ed_table.table_name)


def ingest_test_file_data(
    source_project: Project,
    target_connection: Connection,
    transform_dt_col: bool = True,
    dtype: Dict[str, Any] = None,
) -> SQLAlchemyAdapter:
    source_adapter = cast(FileAdapter, source_project.get_adapter())

    target_source = Project(
        connection=target_connection, event_data_tables=source_project.event_data_tables
    )
    target_adapter = cast(SQLAlchemyAdapter, target_source.get_adapter())
    target_engine = target_adapter.get_engine()

    for ed_table in source_project.event_data_tables:
        pdf = source_adapter._read_file(ed_table)
        if transform_dt_col:
            pdf[ed_table.event_time_field] = pdf[ed_table.event_time_field].apply(
                lambda v: datetime.fromisoformat(v)
            )

        pdf.to_sql(
            con=target_engine,
            name=ed_table.table_name,
            index=False,
            dtype=dtype,
            if_exists="replace",
        )

    return target_adapter
