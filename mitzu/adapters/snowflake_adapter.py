from __future__ import annotations


import mitzu.model as M
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter, FieldReference
from typing import Any, List
from snowflake.sqlalchemy.custom_types import TIMESTAMP_NTZ, TIMESTAMP_TZ
import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP
import sqlalchemy.sql.sqltypes as SA_T
import mitzu.adapters.generic_adapter as GA
import pandas as pd
import ast


class SnowflakeAdapter(SQLAlchemyAdapter):
    def _get_connection_url(self, con: M.Connection):
        url = super()._get_connection_url(con)
        extra_args = {}

        if "warehouse" in con.extra_configs.keys():
            extra_args["warehouse"] = con.extra_configs["warehouse"]

        if "role" in con.extra_configs.keys():
            extra_args["role"] = con.extra_configs["role"]

        if len(extra_args) > 0:
            url += "?" + "&".join(
                f"{key}={value}" for (key, value) in extra_args.items()
            )

        return url

    def map_type(self, sa_type: Any) -> M.DataType:
        if type(sa_type) in [TIMESTAMP_NTZ, TIMESTAMP_TZ]:
            return M.DataType.DATETIME

        return super().map_type(sa_type)

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return SA.func.dateadd(
            timewindow.period.name.lower(),
            timewindow.value,
            field_ref,
        )

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        return SA.func.dateadd(time_group.name.lower(), value_field_ref, field_ref)

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> pd.DataFrame:
        df = super()._get_column_values_df(event_data_table, fields, event_specific)
        for field in df.columns:
            df[field] = df[field].apply(
                lambda val: ast.literal_eval(val) if val is not None else None
            )
        return df

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:
        if metric._agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
            if metric._agg_param is None or 0 < metric._agg_param > 100:
                raise ValueError(
                    "Conversion percentile parameter must be between 0 and 100"
                )
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            return SA.cast(
                SA.func.approx_percentile(
                    SA.func.datediff("second", t1, t2), metric._agg_param / 100.0
                ),
                SA_T.FLOAT,
            )
        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            return SA.cast(SA.func.avg(SA.func.datediff("second", t1, t2)), SA_T.FLOAT)
        else:
            return super()._get_conv_aggregation(metric, cte, first_cte)
