from __future__ import annotations
from typing import Any, List

from mitzu.adapters.slqalchemy_adapter import SQLAlchemyAdapter
import mitzu.common.model as M
import pandas as pd  # type: ignore
import sqlalchemy as SA  # type: ignore
import json

VALUE_SEPARATOR = "###"


class SQLiteAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def get_engine(self) -> Any:
        if self._engine is None:
            source = self.source
            extension = source.connection.connection_params["file_type"]
            path = source.connection.connection_params["path"]
            if extension == "sqlite":
                eng = SA.create_engine(f"sqlite://{path}")
            else:
                if extension == "csv":
                    df = pd.read_csv(path, header=0)
                elif extension == "json":
                    df = pd.read_json(path)
                elif extension == "parquet":
                    df = pd.read_parquet(path)
                else:
                    raise Exception("Extension not supported: " + extension)
                df[source.event_time_field] = pd.to_datetime(
                    df[source.event_time_field]
                )
                df = self._fix_complex_types(df)
                eng = SA.create_engine("sqlite://")
                df.to_sql(name=source.table_name, con=eng, index=False)
                eng.execute(
                    SA.text(
                        f"""CREATE INDEX {source.table_name}_index 
                            ON {source.table_name} (
                            {source.user_id_field}, 
                            {source.event_name_field}, 
                            {source.event_time_field})"""
                    )
                )
            self._engine = eng
        return self._engine

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

    def _column_index_support(self):
        return False

    def _get_date_trunc(self, time_group: M.TimeGroup, table_column: SA.Column):
        if time_group == M.TimeGroup.WEEK:
            return SA.func.datetime(SA.func.date(table_column, "weekday 0", "-6 days"))
        if time_group == M.TimeGroup.SECOND:
            fmt = "%Y-%m-%dT%H:%M:%S"
        elif time_group == M.TimeGroup.MINUTE:
            fmt = "%Y-%m-%dT%H:%M:00"
        elif time_group == M.TimeGroup.HOUR:
            fmt = "%Y-%m-%dT%H:00:00"
        elif time_group == M.TimeGroup.DAY:
            fmt = "%Y-%m-%dT00:00:00"
        elif time_group == M.TimeGroup.WEEK:
            fmt = "%Y-%m-%dT00:00:00"
        elif time_group == M.TimeGroup.MONTH:
            fmt = "%Y-%m-01T00:00:00"
        elif time_group == M.TimeGroup.QUARTER:
            raise NotImplementedError(
                "Timegroup Quarter is not supported for SQLite Adapter"
            )
        elif time_group == M.TimeGroup.YEAR:
            fmt = "%Y-01-01T00:00:00"

        return SA.func.datetime(SA.func.strftime(fmt, table_column))

    def _get_column_values_df(
        self, fields: List[M.Field], event_specific: bool
    ) -> pd.DataFrame:
        source = self.source
        df = super()._get_column_values_df(fields, event_specific)

        for field in df.columns:
            if field != source.event_name_field:
                df[field] = (
                    df[field]
                    .str.replace(f"{VALUE_SEPARATOR}$", "", regex=True)
                    .str.split(f"{VALUE_SEPARATOR},")
                )
        return df

    def _get_distinct_array_agg_func(self, column: SA.Column) -> Any:
        return SA.func.group_concat(column.concat(VALUE_SEPARATOR).distinct())

    def _get_datetime_interval(
        self, table_column: SA.Column, timewindow: M.TimeWindow
    ) -> Any:
        return SA.func.datetime(
            table_column, f"+{timewindow.value} {timewindow.period.name.lower()}"
        )
