from __future__ import annotations
from typing import Any, List

from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
import mitzu.adapters.generic_adapter as GA
import mitzu.common.model as M
import pandas as pd  # type: ignore
import sqlalchemy as SA  # type: ignore
from mitzu.adapters.helper import dataframe_str_to_datetime

VALUE_SEPARATOR = "###"


class SQLiteAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        df = super().get_conversion_df(metric)
        return dataframe_str_to_datetime(df, GA.DATETIME_COL)

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        df = super().get_segmentation_df(metric)
        return dataframe_str_to_datetime(df, GA.DATETIME_COL)

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
            if field != source.event_data_table.event_name_field:
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
