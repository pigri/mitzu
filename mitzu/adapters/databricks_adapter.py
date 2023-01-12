from __future__ import annotations

from typing import Any, List, cast

import mitzu.adapters.generic_adapter as GA
import mitzu.adapters.sqlalchemy.databricks.sqlalchemy.datatype as DA_T
import mitzu.model as M
from mitzu.adapters.sqlalchemy.databricks import sqlalchemy  # noqa: F401
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter, FieldReference
from mitzu.helper import LOGGER

import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP
import sqlalchemy.sql.sqltypes as SA_T
from sqlalchemy.sql.type_api import TypeEngine


class DatabricksAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def get_engine(self) -> Any:
        con = self.project.connection
        if self._engine is None:
            if con.url is None:
                url = self._get_connection_url(con)
            else:
                url = con.url
            http_path = con.extra_configs.get("http_path")
            if http_path is None:
                raise Exception(
                    "Connection extra_configs must contain http_path. (extra_configs={'http_path':'<path>'}"
                )
            self._engine = SA.create_engine(url, connect_args={"http_path": http_path})
        return self._engine

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
            F.flatten(F.collect_set(F.map_keys(cte.columns[name])))
        )

        max_cardinality = event_data_table.discovery_settings.max_map_key_cardinality
        q = SA.select(
            columns=[
                SA.case(
                    [(F.size(map_keys_func) < max_cardinality, map_keys_func)],
                    else_=None,
                ).label("sub_fields")
            ]
        )
        df = self.execute_query(q)
        if df.shape[0] == 0:
            # The map typa has no values
            return M.Field(_name=name, _type=M.DataType.MAP)
        if df.iat[0, 0] is None:
            # The total amount of map keys are more than the allowed from the EDT
            keys = []
        else:
            keys = df.iat[0, 0].tolist()

        sf_type = self.map_type(map.value_type)
        sub_fields: List[M.Field] = [M.Field(key, sf_type) for key in keys]
        return M.Field(_name=name, _type=M.DataType.MAP, _sub_fields=tuple(sub_fields))

    def _get_struct_type(self) -> TypeEngine:
        return DA_T.STRUCT

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
            return SA.func.percentile_approx(
                SA.cast(t2 - t1, SA_T.BIGINT), metric._agg_param / 100.0, 100
            )
        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            return SA.func.avg(SA.cast(t2 - t1, SA_T.BIGINT))
        else:
            return super()._get_conv_aggregation(metric, cte, first_cte)

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        if time_group == M.TimeGroup.SECOND:
            return SA.func.from_unixtime(
                SA.func.unix_timestamp(field_ref) + value_field_ref
            )
        if time_group == M.TimeGroup.MINUTE:
            return SA.func.from_unixtime(
                SA.func.unix_timestamp(field_ref) + value_field_ref * 60
            )
        if time_group == M.TimeGroup.HOUR:
            SA.func.from_unixtime(
                SA.func.unix_timestamp(field_ref) + value_field_ref * 3600
            )
        if time_group == M.TimeGroup.DAY:
            return SA.func.date_add(field_ref, value_field_ref)
        if time_group == M.TimeGroup.WEEK:
            return SA.func.date_add(field_ref, value_field_ref * 7)
        if time_group == M.TimeGroup.MONTH:
            return SA.func.add_months(field_ref, value_field_ref)
        if time_group == M.TimeGroup.QUARTER:
            return SA.func.add_months(field_ref, value_field_ref * 4)
        if time_group == M.TimeGroup.YEAR:
            return SA.func.add_months(field_ref, value_field_ref * 12)

        raise ValueError(
            f"{time_group} is not supported for databricks dynamic time intervals"
        )
