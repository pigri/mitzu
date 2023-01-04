from __future__ import annotations

from datetime import datetime
from typing import Any, List, Union, cast

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
import trino.sqlalchemy.datatype as SA_T
from mitzu.adapters.helper import dataframe_str_to_datetime, pdf_string_array_to_array
from mitzu.adapters.sqlalchemy_adapter import FieldReference, SQLAlchemyAdapter
from mitzu.helper import LOGGER
from sqlalchemy.sql.type_api import TypeEngine
import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP


class TrinoAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        df = super().get_conversion_df(metric)
        df = dataframe_str_to_datetime(df, GA.DATETIME_COL)
        for index in range(1, len(metric._conversion._segments) + 1):
            df[f"{GA.AGG_VALUE_COL}_{index}"] = df[
                f"{GA.AGG_VALUE_COL}_{index}"
            ].astype(float)
        return df

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        df = super().get_segmentation_df(metric)
        df = dataframe_str_to_datetime(df, GA.DATETIME_COL)
        df[GA.AGG_VALUE_COL] = df[GA.AGG_VALUE_COL].astype(float)
        return df

    def get_retention_df(self, metric: M.RetentionMetric) -> pd.DataFrame:
        df = super().get_retention_df(metric)
        df = dataframe_str_to_datetime(df, GA.GROUP_COL)
        df = dataframe_str_to_datetime(df, GA.DATETIME_COL)
        df[GA.AGG_VALUE_COL] = df[GA.AGG_VALUE_COL].astype(float)
        return df

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:
            query = str(query.compile(compile_kwargs={"literal_binds": True}))
        return super().execute_query(query=query)

    def get_field_reference(
        self,
        field: M.Field,
        event_data_table: M.EventDataTable = None,
        sa_table: Union[SA.Table, EXP.CTE] = None,
    ) -> FieldReference:
        if sa_table is None and event_data_table is not None:
            sa_table = self.get_table(event_data_table)
        if sa_table is None:
            raise ValueError("Either sa_table or event_data_table has to be provided")

        if field._parent is not None and field._parent._type == M.DataType.MAP:
            map_key = field._name
            property = SA.literal_column(f"{sa_table.name}.{field._parent._get_name()}")
            return SA.func.element_at(property, map_key)

        return super().get_field_reference(field, event_data_table, sa_table)

    def map_type(self, sa_type: Any) -> M.DataType:
        if isinstance(sa_type, SA_T.ROW):
            return M.DataType.STRUCT
        if isinstance(sa_type, SA_T.MAP):
            return M.DataType.MAP
        return super().map_type(sa_type)

    def _parse_map_type(
        self,
        sa_type: Any,
        name: str,
        event_data_table: M.EventDataTable,
    ) -> M.Field:
        if event_data_table.discovery_settings is None:
            raise ValueError("Missing discovery settings")

        LOGGER.debug(f"Discovering map: {name}")
        map: SA_T.MAP = cast(SA_T.MAP, sa_type)
        if map.value_type in (SA_T.ROW, SA_T.MAP):
            raise Exception(
                f"Compounded map types are not supported: map<{map.key_type}, {map.value_type}>"
            )
        cte = self._get_dataset_discovery_cte(event_data_table)
        F = SA.func
        map_keys_func = F.array_distinct(
            F.flatten(F.array_agg(F.distinct(F.map_keys(cte.columns[name]))))
        )

        max_cardinality = event_data_table.discovery_settings.max_map_key_cardinality
        q = SA.select(
            columns=[
                SA.case(
                    [(F.cardinality(map_keys_func) < max_cardinality, map_keys_func)],
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

    def _generate_time_series_column(self, dt: datetime) -> Any:
        return SA.literal_column(f"timestamp '{dt}'")

    def _get_struct_type(self) -> TypeEngine:
        return SA_T.ROW

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
        return SA.text(f"timestamp '{dt}'")

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
