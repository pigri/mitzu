from __future__ import annotations

import json
from datetime import datetime
from typing import cast

import mitzu.model as M
import pandas as pd
import sqlalchemy as SA
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
from mitzu.helper import LOGGER
from mitzu.samples.sample_data_generator import create_all_funnels


def ingest_dataframe(
    engine: SA.engine.Engine, table_name: str, df: pd.DataFrame, recreate: bool
):
    ins = SA.inspect(engine)
    if ins.dialect.has_table(engine.connect(), table_name):
        LOGGER.warn(f"Table {table_name} already exists")
        if not recreate:
            return

    LOGGER.debug(f"Ingesting records into {table_name}")
    df.to_sql(
        con=engine,
        name=table_name,
        index=False,
        if_exists="replace",
    )


def flatten_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    datetime_cols = []
    for col in df.dtypes.index:
        if df.dtypes[col].name == "datetime64[ns]":
            datetime_cols.append(col)

    json_struct = json.loads(df.to_json(orient="records"))
    norm_df = pd.json_normalize(json_struct)
    for col in datetime_cols:
        norm_df[col] = df[col]
    return norm_df


def create_and_ingest_sample_project(
    connection: M.Connection,
    event_count: int = 100000,
    number_of_users: int = 1000,
    overwrite_records: bool = True,
) -> M.Project:
    dfs = create_all_funnels(event_count=event_count, user_count=number_of_users)
    event_data_tables = []
    for key, df in dfs.items():
        event_data_tables.append(
            M.EventDataTable.create(
                table_name=key,
                event_time_field="event_time",
                user_id_field="user_id",
                event_name_alias=key if "event_name" not in df.columns else None,
                event_name_field="event_name" if "event_name" in df.columns else None,
            )
        )
    project = M.Project(
        connection=connection,
        event_data_tables=event_data_tables,
        default_discovery_lookback_days=365,
        default_end_dt=datetime(2022, 1, 1),
    )

    for key, df in dfs.items():
        adapter = project.get_adapter()
        engine = cast(SQLAlchemyAdapter, adapter).get_engine()
        ingest_dataframe(engine, key, df, overwrite_records)

    return project


if __name__ == "__main__":
    dfs = create_all_funnels(event_count=500000, user_count=2500)
    for name, df in dfs.items():
        df.to_parquet(f"{name}.parquet")
