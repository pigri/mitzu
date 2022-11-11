from __future__ import annotations

from typing import Any

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
from mitzu.adapters.sqlalchemy_adapter import (
    FieldReference,
    SQLAlchemyAdapter,
)

import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP


class PostgresqlAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return field_ref + SA.text(f"interval '{timewindow.value} {timewindow.period}'")

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        return field_ref + (value_field_ref * SA.text(f"interval '1 {time_group}'"))

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:
        if metric._agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
            raise NotImplementedError(
                "Percentile calculation is not supported by PostgreSQL Connections."
            )
        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            diff = SA.func.extract("EPOCH", t2) - SA.func.extract("EPOCH", t1)
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
