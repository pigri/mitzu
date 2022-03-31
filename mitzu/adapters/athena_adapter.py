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
from mitzu.adapters.helper import pdf_string_array_to_array


class AthenaAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def get_engine(self) -> Any:
        if self._engine is None:
            params = self.source.connection.connection_params
            conn_str = (
                "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}"
                "@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}"
            )

            engine = create_engine(
                conn_str.format(
                    aws_access_key_id=quote_plus(params["aws_access_key_id"]),
                    aws_secret_access_key=quote_plus(params["aws_secret_access_key"]),
                    region_name=quote_plus(params["region_name"]),
                    schema_name=quote_plus(params.get("schema_name", "default")),
                    s3_staging_dir=quote_plus(params["s3_staging_dir"]),
                )
            )
            #
            engine.dialect.description_encoding = None

            self._engine = engine
        return self._engine

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:
            # PyAthena has a bug that the query needs to be compiled and casted to string before execution
            query = format_sql(
                str(query.compile(compile_kwargs={"literal_binds": True}))
            )
        return super().execute_query(query=query)

    def _get_column_values_df(
        self, fields: List[M.Field], event_specific: bool
    ) -> pd.DataFrame:
        df = super()._get_column_values_df(fields=fields, event_specific=event_specific)
        return pdf_string_array_to_array(df)

    def _get_timewindow_where_clause(self, table: SA.Table, metric: M.Metric) -> Any:
        start_date = metric._start_dt.replace(microsecond=0)
        end_date = metric._end_dt.replace(microsecond=0)

        evt_time_col = table.columns.get(self.source.event_time_field)
        return (evt_time_col >= SA.text(f"timestamp '{start_date}'")) & (
            evt_time_col <= SA.text(f"timestamp '{end_date}'")
        )
