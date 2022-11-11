from __future__ import annotations

from typing import Any, List

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
from mitzu.adapters.helper import pdf_string_array_to_array
from mitzu.adapters.sqlalchemy_adapter import FieldReference, SQLAlchemyAdapter

import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP

NULL_VALUE_KEY = "##NULL##"


class MySQLAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def _get_distinct_array_agg_func(self, field_ref: FieldReference) -> Any:
        return SA.func.json_keys(
            SA.func.json_objectagg(SA.func.coalesce(field_ref, NULL_VALUE_KEY), "")
        )

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return field_ref + SA.text(f"interval {timewindow.value} {timewindow.period}")

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        return field_ref + SA.text(f"interval {value_field_ref} {time_group}")

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> pd.DataFrame:
        df = super()._get_column_values_df(
            event_data_table=event_data_table,
            fields=fields,
            event_specific=event_specific,
        )
        df = pdf_string_array_to_array(df, split_text='", "', omit_chars=2)
        return df

    def _get_date_trunc(self, time_group: M.TimeGroup, field_ref: FieldReference):
        if time_group == M.TimeGroup.WEEK:
            return SA.func.date_add(
                SA.func.date(field_ref),
                EXP.text(f"interval -{SA.func.weekday(field_ref)} day"),
            )

        elif time_group == M.TimeGroup.SECOND:
            fmt = "%Y-%m-%dT%H:%i:%S"
        elif time_group == M.TimeGroup.MINUTE:
            fmt = "%Y-%m-%dT%H:%i:00"
        elif time_group == M.TimeGroup.HOUR:
            fmt = "%Y-%m-%dT%H:00:00"
        elif time_group == M.TimeGroup.DAY:
            fmt = "%Y-%m-%d"
        elif time_group == M.TimeGroup.MONTH:
            fmt = "%Y-%m-01"
        elif time_group == M.TimeGroup.QUARTER:
            raise NotImplementedError(
                "Timegroup Quarter is not supported for MySQL Adapter"
            )
        elif time_group == M.TimeGroup.YEAR:
            fmt = "%Y-01-01"

        return SA.func.timestamp(SA.func.date_format(field_ref, fmt))

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:
        if metric._agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
            raise NotImplementedError(
                "Percentile calculation is not supported by MySQL Connections."
            )
        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            diff = SA.func.unix_timestamp(t2) - SA.func.unix_timestamp(t1)
            return SA.func.avg(diff)
        else:
            return super()._get_conv_aggregation(metric, cte, first_cte)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        df = super().get_conversion_df(metric)
        for index in range(1, len(metric._conversion._segments) + 1):
            df[f"{GA.AGG_VALUE_COL}_{index}"] = df[
                f"{GA.AGG_VALUE_COL}_{index}"
            ].astype(float)
        return df

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        df = super().get_segmentation_df(metric)
        df[GA.AGG_VALUE_COL] = df[GA.AGG_VALUE_COL].astype(float)
        return df
