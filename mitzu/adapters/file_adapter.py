from __future__ import annotations
from typing import Any

from mitzu.adapters.sqlite_adapter import SQLiteAdapter
import mitzu.common.model as M
import pandas as pd  # type: ignore
import json

VALUE_SEPARATOR = "###"


class FileAdapter(SQLiteAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def _get_connection_url(self, con: M.Connection):
        return "sqlite://"

    def get_engine(self) -> Any:
        if self._engine is None:
            source = self.source
            df = self._read_file()
            self._engine = super().get_engine()
            df.to_sql(
                name=source.event_data_table.table_name,
                con=self._engine,
                index=False,
            )
        return self._engine

    def _read_file(self) -> pd.DataFrame:
        source = self.source
        extension = source.connection.extra_configs["file_type"]
        path = source.connection.extra_configs["path"]
        if extension == "csv":
            df = pd.read_csv(path, header=0)
        elif extension == "json":
            df = pd.read_json(path)
        elif extension == "parquet":
            df = pd.read_parquet(path)
        else:
            raise Exception("Extension not supported: " + extension)
        df[source.event_data_table.event_time_field] = pd.to_datetime(
            df[source.event_data_table.event_time_field]
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
