from __future__ import annotations

from datetime import datetime
from typing import Any, List

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
import sqlalchemy as SA
from mitzu.adapters.helper import pdf_string_array_to_array
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
from sqlalchemy.sql.expression import CTE


class AthenaAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:
            query = str(query.compile(compile_kwargs={"literal_binds": True}))
        return super().execute_query(query=query)

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
        return pdf_string_array_to_array(df)

    def _correct_timestamp(self, dt: datetime) -> Any:
        timeformat = dt.strftime("%Y-%m-%d %H:%M:%S")
        return SA.text(f"timestamp '{timeformat}'")

    def _get_timewindow_where_clause(self, cte: CTE, metric: M.Metric) -> Any:
        start_date = metric._start_dt.replace(microsecond=0)
        end_date = metric._end_dt.replace(microsecond=0)

        evt_time_col = cte.columns.get(GA.CTE_DATETIME_COL)
        return (evt_time_col >= self._correct_timestamp(start_date)) & (
            evt_time_col <= self._correct_timestamp(end_date)
        )
