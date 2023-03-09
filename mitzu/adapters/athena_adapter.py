from __future__ import annotations

from datetime import datetime
from typing import Any, List, cast

import mitzu.adapters.generic_adapter as GA
import mitzu.adapters.sqlalchemy.athena.sqlalchemy.datatype as DA_T
import mitzu.model as M
import pandas as pd
from mitzu.adapters.helper import pdf_string_json_array_to_array
from mitzu.adapters.sqlalchemy.athena import sqlalchemy  # noqa: F401
from mitzu.adapters.sqlalchemy_adapter import (
    FieldReference,
    SQLAlchemyAdapter,
)
from mitzu.helper import LOGGER
from sqlalchemy.sql.type_api import TypeEngine
import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP


class AthenaAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:
            query = str(query.compile(compile_kwargs={"literal_binds": True}))
            query = query.replace(
                "%", "%%"
            )  # bugfix for pyathena, which has string formatting
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
        return pdf_string_json_array_to_array(df)

    def _correct_timestamp(self, dt: datetime) -> Any:
        timeformat = dt.strftime("%Y-%m-%d %H:%M:%S")
        return SA.text(f"timestamp '{timeformat}'")

    def map_type(self, sa_type: Any) -> M.DataType:
        if isinstance(sa_type, DA_T.MAP):
            return M.DataType.MAP
        if isinstance(sa_type, DA_T.STRUCT):
            return M.DataType.STRUCT
        return super().map_type(sa_type)

    def _parse_map_type(
        self, sa_type: Any, name: str, event_data_table: M.EventDataTable
    ) -> M.Field:
        if event_data_table.discovery_settings is None:
            raise ValueError("Missing discovery settings")

        LOGGER.debug(f"Discovering map: {name}")
        map: DA_T.MAP = cast(DA_T.MAP, sa_type)
        if map.value_type in (DA_T.STRUCT, DA_T.MAP):
            raise Exception(
                f"Compounded map types are not supported: map<{map.key_type}, {map.value_type}>"
            )
        cte = self._get_dataset_discovery_cte(event_data_table)
        F = SA.func
        map_keys_func = F.array_distinct(
            F.flatten(F.array_agg(F.map_keys(cte.columns[name]).distinct()))
        )

        max_cardinality = event_data_table.discovery_settings.max_map_key_cardinality
        q = SA.select(
            columns=[
                SA.case(
                    [
                        (
                            F.cardinality(map_keys_func) < max_cardinality,
                            F.cast(map_keys_func, SA.JSON),
                        )
                    ],
                    else_=None,
                ).label("sub_fields")
            ]
        )
        df = self.execute_query(q)
        if df.shape[0] == 0:
            return M.Field(_name=name, _type=M.DataType.MAP)
        keys = df.iat[0, 0]
        sf_type = self.map_type(map.value_type)
        sub_fields: List[M.Field] = [M.Field(key, sf_type) for key in keys]
        return M.Field(_name=name, _type=M.DataType.MAP, _sub_fields=tuple(sub_fields))

    def _get_struct_type(self) -> TypeEngine:
        return DA_T.STRUCT

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return SA.func.date_add(
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
        return SA.func.date_add(time_group.name.lower(), value_field_ref, field_ref)

    def _generate_time_series_column(self, dt: datetime) -> Any:
        dt_str = datetime.strftime(dt, "%Y-%m-%d %H:%M:%S.%f")
        return SA.literal_column(f"timestamp '{dt_str}'")

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
            return SA.func.approx_percentile(
                SA.func.date_diff("second", t1, t2), metric._agg_param / 100.0
            )
        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            return SA.func.avg(SA.func.date_diff("second", t1, t2))
        else:
            return super()._get_conv_aggregation(metric, cte, first_cte)
