from __future__ import annotations

from typing import Any, List

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
from mitzu.adapters.helper import dataframe_str_to_datetime
from mitzu.adapters.sqlalchemy_adapter import (
    FieldReference,
    SQLAlchemyAdapter,
)

import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP

ARRAY_JOIN_SEP = "###"


class SQLiteAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def _column_index_support(self):
        return False

    def _get_connection_url(self, con: M.Connection):
        if not con.host:
            return "sqlite://?check_same_thread=False"
        else:
            return f"sqlite:///{con.host}.db?check_same_thread=False"

    def keep_alive_connection(self) -> bool:
        return True

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
                .str.replace(f"{ARRAY_JOIN_SEP}$", "", regex=True)
                .str.split(f"{ARRAY_JOIN_SEP},")
            )
        return df

    def _get_distinct_array_agg_func(self, field_ref: FieldReference) -> Any:
        return SA.func.group_concat(SA.distinct(field_ref.concat(ARRAY_JOIN_SEP)))

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

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        if time_group == M.TimeGroup.WEEK:
            # SQL Lite doesn't have the week concept
            time_group = M.TimeGroup.DAY
            return SA.func.datetime(
                field_ref,
                SA.text(
                    f"'+' || ({value_field_ref} * 7) || ' {time_group.name.lower()}'"
                ),
            )

        return SA.func.datetime(
            field_ref,
            SA.text(f"'+' || {value_field_ref} || ' {time_group.name.lower()}'"),
        )

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:
        if metric._agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
            raise NotImplementedError(
                "Percentile calculation is not supported by SQLite Connections."
            )
        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            diff = SA.func.round(
                (SA.func.julianday(t2) - SA.func.julianday(t1)) * 86400
            )
            return SA.func.avg(diff)
        else:
            return super()._get_conv_aggregation(metric, cte, first_cte)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        df = super().get_conversion_df(metric)
        return dataframe_str_to_datetime(df, GA.DATETIME_COL)

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        df = super().get_segmentation_df(metric)
        return dataframe_str_to_datetime(df, GA.DATETIME_COL)

    def get_retention_df(self, metric: M.RetentionMetric) -> pd.DataFrame:
        df = super().get_retention_df(metric)
        df = dataframe_str_to_datetime(df, GA.DATETIME_COL)
        return df

    def _get_datetime_column(self, cte: Any, name: str) -> Any:
        return SA.func.datetime(cte.columns.get(name))
