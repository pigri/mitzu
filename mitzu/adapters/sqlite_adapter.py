from __future__ import annotations

from typing import Any, List

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
from mitzu.adapters.helper import dataframe_str_to_datetime
from mitzu.adapters.sqlalchemy_adapter import FieldReference, SQLAlchemyAdapter

import sqlalchemy as SA

VALUE_SEPARATOR = "###"


class SQLiteAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        df = super().get_conversion_df(metric)
        return dataframe_str_to_datetime(df, GA.DATETIME_COL)

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        df = super().get_segmentation_df(metric)
        return dataframe_str_to_datetime(df, GA.DATETIME_COL)

    def _column_index_support(self):
        return False

    def _get_date_trunc(self, time_group: M.TimeGroup, field_ref: FieldReference):
        if time_group == M.TimeGroup.WEEK:
            return SA.func.datetime(SA.func.date(field_ref, "weekday 0", "-6 days"))
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

        return SA.func.datetime(SA.func.strftime(fmt, field_ref))

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> pd.DataFrame:
        df = super()._get_column_values_df(event_data_table, fields, event_specific)

        for field in df.columns:
            df[field] = (
                df[field]
                .str.replace(f"{VALUE_SEPARATOR}$", "", regex=True)
                .str.split(f"{VALUE_SEPARATOR},")
            )
        return df

    def _get_distinct_array_agg_func(self, field_ref: FieldReference) -> Any:
        return SA.func.group_concat(SA.distinct(field_ref.concat(VALUE_SEPARATOR)))

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        if timewindow.period == M.TimeGroup.WEEK:
            # SQL Lite doesn't have the week concept
            timewindow = M.TimeWindow(timewindow.value * 7, M.TimeGroup.DAY)
        return SA.func.datetime(
            field_ref,
            SA.text(f"'+{timewindow.value} {timewindow.period.name.lower()}'"),
        )
