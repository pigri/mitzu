from __future__ import annotations

from typing import Any, List

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
