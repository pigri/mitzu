from __future__ import annotations
from typing import Any, List

from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
import mitzu.common.model as M
import pandas as pd  # type: ignore
import sqlalchemy as SA  # type: ignore
import sqlalchemy.sql.expression as EXP  # type: ignore
from mitzu.adapters.helper import pdf_string_array_to_array

NULL_VALUE_KEY = "##NULL##"


class MySQLAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def _get_distinct_array_agg_func(self, column: SA.Column) -> Any:
        return SA.func.json_keys(
            SA.func.json_objectagg(SA.func.coalesce(column, NULL_VALUE_KEY), "")
        )

    def _get_column_values_df(
        self, fields: List[M.Field], event_specific: bool
    ) -> pd.DataFrame:
        df = super()._get_column_values_df(fields=fields, event_specific=event_specific)
        df = pdf_string_array_to_array(df, split_text='", "', omit_chars=2)
        return df

    def _get_date_trunc(self, time_group: M.TimeGroup, table_column: SA.Column):
        if time_group == M.TimeGroup.WEEK:
            return SA.func.date_add(
                SA.func.date(table_column),
                EXP.text(f"interval -weekday({table_column}) day"),
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

        return SA.func.timestamp(SA.func.date_format(table_column, fmt))
