from __future__ import annotations

from typing import Any, List, Union
from mitzu.adapters.sqlalchemy.bigquery import sqlalchemy  # noqa: F401

import mitzu.model as M
import pandas as pd
from mitzu.adapters.sqlalchemy_adapter import (
    SQLAlchemyAdapter,
    FieldReference,
)
import mitzu.adapters.generic_adapter as GA
import sqlalchemy as SA
from mitzu.adapters.helper import pdf_string_json_array_to_array

import sqlalchemy.sql.expression as EXP
from mitzu.adapters.sqlalchemy.bigquery.sqlalchemy import STRUCT
from sqlalchemy.sql.type_api import TypeEngine
import sqlalchemy.sql.sqltypes as SA_T
from mitzu.helper import LOGGER


JSON_LIST_KEY_VALS = '''
    CREATE TEMP FUNCTION
    json_object_list(input JSON)
    RETURNS ARRAY<String>
    LANGUAGE js AS """
    return Object.keys(input);
    """;    

    with prop_keys as ( 
        SELECT ARRAY_CONCAT_AGG(json_object_list({field_name}) ) list
        FROM {table}
    )

    SELECT distinct l FROM prop_keys, UNNEST(list) AS l LIMIT {limit}
'''


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
            query = query.replace(
                "%", "%%"
            )  # bugfix for pyathena, which has string formatting
        return super().execute_query(query=query)

    def get_field_reference(
        self,
        field: M.Field,
        event_data_table: M.EventDataTable = None,
        sa_table: Union[SA.Table, EXP.CTE] = None,
    ):
        if sa_table is None and event_data_table is not None:
            sa_table = self.get_table(event_data_table)
        if sa_table is None:
            raise ValueError("Either sa_table or event_data_table has to be provided")

        if field._parent is None:
            res = sa_table.columns.get(field._name)
        elif field._parent._type == M.DataType.MAP:
            res = SA.literal_column(
                f"json_value({sa_table.name}.{field._parent._get_name()}['{field._name}'])"
            )
        else:
            res = SA.literal_column(f"{sa_table.name}.{field._get_name()}")

        if field._type == M.DataType.DATETIME:
            res = SA.func.datetime(res)
        return res

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

    def map_type(self, sa_type: Any) -> M.DataType:
        if isinstance(sa_type, STRUCT):
            return M.DataType.STRUCT
        if isinstance(sa_type, SA_T.JSON):
            return M.DataType.MAP
        return super().map_type(sa_type)

    def _get_struct_sub_types(self, struct_val: Any) -> Any:
        return struct_val._STRUCT_fields

    def _parse_map_type(
        self,
        sa_type: Any,
        name: str,
        event_data_table: M.EventDataTable,
    ) -> M.Field:
        if event_data_table.discovery_settings is None:
            raise ValueError("Missing discovery settings")

        LOGGER.debug(f"Discovering JSON (Map): {name}")

        max_cardinality = event_data_table.discovery_settings.max_map_key_cardinality
        try:
            query = JSON_LIST_KEY_VALS.format(
                table=event_data_table.get_full_name(),
                field_name=name,
                limit=max_cardinality + 1,
            )

            df = self.execute_query(SA.text(query))
        except Exception as exc:
            if (
                "Cannot convert undefined or null to object at json_object_list(JSON)"
                not in str(exc)
            ):
                raise exc
            df = pd.DataFrame()

        if df.shape[0] == 0 or df.shape[0] > max_cardinality:
            return M.Field(_name=name, _type=M.DataType.MAP)

        keys = df.to_dict(orient="list")["l"]
        sub_fields: List[M.Field] = [M.Field(key, M.DataType.STRING) for key in keys]

        return M.Field(_name=name, _type=M.DataType.MAP, _sub_fields=tuple(sub_fields))

    def _get_struct_type(self) -> TypeEngine:
        return STRUCT
