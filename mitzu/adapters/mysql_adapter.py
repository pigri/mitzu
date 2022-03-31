from __future__ import annotations
from typing import Any, List

import mitzu.adapters.generic_adapter as GA
from mitzu.adapters.slqalchemy_adapter import SQLAlchemyAdapter
import mitzu.common.model as M
from urllib.parse import quote_plus
from sqlalchemy.engine import create_engine  # type: ignore
import pandas as pd  # type: ignore
from sql_formatter.core import format_sql  # type: ignore
import sqlalchemy as SA  # type: ignore

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
        return df
