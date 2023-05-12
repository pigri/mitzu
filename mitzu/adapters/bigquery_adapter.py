from __future__ import annotations

from typing import Any, List, Union

import mitzu.model as M
import pandas as pd
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter, FieldReference
import mitzu.adapters.generic_adapter as GA
import sqlalchemy as SA
from mitzu.adapters.helper import pdf_string_json_array_to_array

import sqlalchemy.sql.expression as EXP


class BigQueryAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def get_engine(self) -> Any:
        con = self.project.connection
        if self._engine is None:
            if con.url is None:
                url = self._get_connection_url(con)
            else:
                url = con.url
            credentials = con.extra_configs.get("credentials")
            if not credentials:
                raise Exception(
                    "Connection extra_configs must contain credentials json."
                )
            self._engine = SA.create_engine(url, credentials_info=credentials)
        return self._engine

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:
            query = str(query.compile(compile_kwargs={"literal_binds": True}))

        res = super().execute_query(query=query)

        return res

    def get_field_reference(
        self,
        field: M.Field,
        event_data_table: M.EventDataTable = None,
        sa_table: Union[SA.Table, EXP.CTE] = None,
    ):
        field_ref = super().get_field_reference(
            field=field, event_data_table=event_data_table, sa_table=sa_table
        )
        if field._type == M.DataType.DATETIME:
            field_ref = SA.func.datetime(field_ref)
        return field_ref

    def _get_sample_function(self):
        return SA.func.mod(SA.cast(self._get_random_function() * 1367, SA.Integer), 100)

    def _get_random_function(self):
        return SA.func.rand()

    def _get_distinct_array_agg_func(self, field_ref: FieldReference) -> Any:
        return SA.func.to_json(SA.func.array_agg(SA.distinct(field_ref)))

    def _get_date_trunc(
        self, time_group: M.TimeGroup, field_ref: FieldReference
    ) -> Any:
        return SA.func.date_trunc(field_ref, SA.literal_column(time_group.name))

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
    ) -> pd.DataFrame:
        df = super()._get_column_values_df(
            event_data_table=event_data_table,
            fields=fields,
        )
        return pdf_string_json_array_to_array(df)

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        return SA.func.datetime_add(
            field_ref,
            SA.literal_column(f"interval {value_field_ref} {time_group.name.lower()}"),
        )

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:

        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            return SA.func.avg(SA.func.date_diff(t2, t1, SA.literal_column("second")))
        else:
            return super()._get_conv_aggregation(metric, cte, first_cte)
