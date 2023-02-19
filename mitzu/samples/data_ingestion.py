from __future__ import annotations

import json
from datetime import datetime
from typing import cast, Optional

import mitzu.model as M
import pandas as pd
import sqlalchemy as SA
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
from mitzu.helper import LOGGER, create_unique_id
from mitzu.samples.sample_data_generator import create_all_funnels
from tqdm import tqdm
import sys


def ingest_dataframe(
    engine: SA.engine.Engine,
    table_name: str,
    schema: str,
    df: pd.DataFrame,
    recreate: bool,
    show_progress: bool = False,
    chunk_size: int = 10000,
):
    ins = SA.inspect(engine)
    if ins.dialect.has_table(engine.connect(), table_name, schema=schema):
        LOGGER.warning(f"Table {table_name} already exists")
        if not recreate:
            return

    LOGGER.debug(f"Ingesting records into {table_name}")
    list_df = [
        df[i : i + chunk_size] for i in range(0, df.shape[0], chunk_size)  # noqa: E203
    ]

    mode = "replace"
    if show_progress:
        list_df = tqdm(
            list_df, leave=False, file=sys.stdout, desc=f"Populating {table_name}"
        )
    for df in list_df:
        df.to_sql(
            con=engine,
            name=table_name,
            schema=schema,
            index=False,
            if_exists=mode,
        )
        mode = "append"


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
    schema: str = "main",
    event_count: int = 100000,
    number_of_users: int = 1000,
    overwrite_records: bool = True,
    show_progress: bool = False,
    chunk_size: int = 10000,
    seed: Optional[int] = 100,
    project_id: Optional[str] = None,
) -> M.Project:
    dfs = create_all_funnels(
        event_count=event_count, user_count=number_of_users, seed=seed
    )
    event_data_tables = []
    for table_name, df in dfs.items():
        event_data_tables.append(
            M.EventDataTable.create(
                table_name=table_name,
                schema=schema,
                event_time_field="event_time",
                user_id_field="user_id",
                event_name_alias=table_name if "event_name" not in df.columns else None,
                event_name_field="event_name" if "event_name" in df.columns else None,
            )
        )
    project = M.Project(
        connection=connection,
        project_name="Sample ecommerce project",
        event_data_tables=event_data_tables,
        discovery_settings=M.DiscoverySettings(
            lookback_days=365,
            end_dt=datetime(2022, 1, 1),
        ),
        webapp_settings=M.WebappSettings(
            end_date_config=M.WebappEndDateConfig.CUSTOM_DATE,
            custom_end_date=datetime(2022, 1, 1),
        ),
        project_id=project_id if project_id is not None else create_unique_id(),
    )

    for table_name, df in dfs.items():
        adapter = project.get_adapter()
        engine = cast(SQLAlchemyAdapter, adapter).get_engine()
        ingest_dataframe(
            engine,
            table_name=table_name,
            schema=schema,
            df=df,
            recreate=overwrite_records,
            show_progress=show_progress,
            chunk_size=chunk_size,
        )

    return project


if __name__ == "__main__":
    dfs = create_all_funnels(event_count=500000, user_count=2500)
    for name, df in dfs.items():
        df.to_parquet(f"{name}.parquet")
