from __future__ import annotations

import json
import pathlib
from typing import Any

import mitzu.model as M
import pandas as pd
from mitzu.adapters.sqlite_adapter import SQLiteAdapter

VALUE_SEPARATOR = "###"


class FileAdapter(SQLiteAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def get_engine(self) -> Any:
        if self._engine is None:
            self._engine = super().get_engine()
            for ed_table in self.project.event_data_tables:
                df = self._read_file(ed_table)
                df.to_sql(
                    name=ed_table.table_name,
                    con=self._engine,
                    index=False,
                )
        return self._engine

    def _read_file(self, event_data_table: M.EventDataTable) -> pd.DataFrame:
        project = self.project
        extension: str = project.connection.extra_configs["file_type"]
        path = project.connection.extra_configs["path"]
        table_name = event_data_table.table_name
        file_loc = pathlib.Path(path, f"{table_name}.{extension}")
        if extension.endswith("csv"):
            df = pd.read_csv(file_loc, header=0)
        elif extension.endswith("json"):
            df = pd.read_json(file_loc)
        elif extension.endswith("parquet"):
            df = pd.read_parquet(
                file_loc,
            )
        else:
            raise Exception("Extension not supported: " + extension)
        df[event_data_table.event_time_field._get_name()] = pd.to_datetime(
            df[event_data_table.event_time_field._get_name()]
        )
        return self._fix_complex_types(df)

    def _fix_complex_types(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            obj = df[col][0]
            if pd.api.types.is_dict_like(obj):
                df[col] = df[col].apply(lambda val: json.dumps(val, default=str))
            elif pd.api.types.is_list_like(obj):
                if type(obj) == tuple:
                    df[col] = df[col].apply(
                        lambda val: json.dumps(dict(val), default=str)
                    )
                else:
                    df[col] = df[col].apply(lambda val: json.dumps(val, default=str))
        return df
