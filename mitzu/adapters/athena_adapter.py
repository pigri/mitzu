from __future__ import annotations
from typing import Any, List

from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
import mitzu.common.model as M
import pandas as pd  # type: ignore
from sql_formatter.core import format_sql  # type: ignore
import sqlalchemy as SA  # type: ignore
from mitzu.adapters.helper import pdf_string_array_to_array


class AthenaAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:
            # PyAthena has a bug that the query needs to be compiled and casted to string before execution
            query = format_sql(
                str(query.compile(compile_kwargs={"literal_binds": True}))
            )
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

    def _get_timewindow_where_clause(
        self, event_data_table: M.EventDataTable, table: SA.Table, metric: M.Metric
    ) -> Any:
        start_date = metric._start_dt.replace(microsecond=0)
        end_date = metric._end_dt.replace(microsecond=0)

        evt_time_col = table.columns.get(event_data_table.event_time_field)
        return (evt_time_col >= SA.text(f"timestamp '{start_date}'")) & (
            evt_time_col <= SA.text(f"timestamp '{end_date}'")
        )
