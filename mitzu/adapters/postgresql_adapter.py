from __future__ import annotations

from typing import Any, Dict

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
from mitzu.adapters.sqlalchemy_adapter import (
    FieldReference,
    SQLAlchemyAdapter,
)

import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP
from mitzu.helper import LOGGER
from typing import Optional, Union
from sqlalchemy.dialects.postgresql.json import JSONB, JSON

SUBFILED_TYPE_SEP = "###"


class PostgresqlAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

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
            # The ->> operator casts every value to String from the JSONB or JSON type
            # However we need to cast the values back to their original types for correct comparison
            operator = "->>"
            if field._type == M.DataType.STRING:
                postgres_type = "::text"
            elif field._type == M.DataType.NUMBER:
                postgres_type = "::numeric"
            elif field._type == M.DataType.BOOL:
                postgres_type = "::boolean"
            else:
                raise ValueError(
                    f"Unsupported json data type for postgres: {field._type}"
                )
            return SA.literal_column(
                f"({sa_table.name}.{field._parent._get_name()} {operator} '{field._name}'){postgres_type}"
            )
        return super().get_field_reference(field, event_data_table, sa_table)

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return field_ref + SA.text(f"interval '{timewindow.value} {timewindow.period}'")

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        return field_ref + (value_field_ref * SA.text(f"interval '1 {time_group}'"))

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:
        if metric._agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
            raise NotImplementedError(
                "Percentile calculation is not supported by PostgreSQL Connections."
            )
        if metric._agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
            t1 = first_cte.columns.get(GA.CTE_DATETIME_COL)
            t2 = cte.columns.get(GA.CTE_DATETIME_COL)
            diff = SA.func.extract("EPOCH", t2) - SA.func.extract("EPOCH", t1)
            return SA.func.avg(diff)
        else:
            return super()._get_conv_aggregation(metric, cte, first_cte)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        df = super().get_conversion_df(metric)
        for index in range(1, len(metric._conversion._segments) + 1):
            df[f"{GA.AGG_VALUE_COL}_{index}"] = df[
                f"{GA.AGG_VALUE_COL}_{index}"
            ].astype(float)
        return df

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        df = super().get_segmentation_df(metric)
        df[GA.AGG_VALUE_COL] = df[GA.AGG_VALUE_COL].astype(float)
        return df

    def _get_distinct_array_agg_func(self, field_ref: FieldReference) -> Any:
        return SA.func.to_json(SA.func.array_agg(SA.distinct(field_ref)))

    def _map_key_type_to_field(self, key_type: str) -> Optional[M.Field]:
        key, str_type = key_type.split(SUBFILED_TYPE_SEP)
        str_type = str_type.lower()

        if str_type == "string":
            data_type = M.DataType.STRING
        elif str_type == "number":
            data_type = M.DataType.NUMBER
        elif str_type == "boolean":
            data_type = M.DataType.BOOL
        else:
            return None
        return M.Field(key, data_type)

    def _parse_map_type(
        self, sa_type: Any, name: str, event_data_table: M.EventDataTable
    ) -> M.Field:
        if event_data_table.discovery_settings is None:
            raise ValueError("Missing discovery settings")

        LOGGER.debug(f"Discovering map: {name}")
        F = SA.func

        if type(sa_type) == JSONB:
            each_func = F.jsonb_each
            type_func = "jsonb_typeof"
        elif type(sa_type) == JSON:
            each_func = F.json_each
            type_func = "json_typeof"
        else:
            raise ValueError(
                f"Unsupported map type for Postgres adapter: {type(sa_type)}"
            )

        cte = self._get_dataset_discovery_cte(event_data_table)
        # SQL Alchemy doesn't support well "record" types. So we need to hack it here
        query = SA.select(
            F.array_remove(
                F.array_agg(
                    SA.literal_column(
                        f"json_data.key || '{SUBFILED_TYPE_SEP}' || {type_func}(json_data.value)"
                    ).distinct()
                ),
                None,
            )
        )
        query = query.select_from(
            cte, SA.lateral(each_func(cte.columns[name])).alias("json_data")
        )

        df = self.execute_query(query)

        if df.shape[0] == 0:
            return M.Field(_name=name, _type=M.DataType.MAP)
        key_types = df.iat[0, 0]

        sub_fields: Dict[str, M.Field] = {}
        for kt in key_types:
            f = self._map_key_type_to_field(kt)
            if f is not None:
                if f._name not in sub_fields:
                    sub_fields[f._name] = f
                else:
                    # If the JSONB or JSON contains keys that have multiple types we treat them as strings
                    # This way Postgres handles all the casting, while on the UI we will have String type like components
                    # We can't just by default consider every jsonb or json value as String because in case the values are only e.g.
                    # only numeric we need to keep the UI to treat it as numeric.
                    sub_fields[f._name] = M.Field(f._name, M.DataType.STRING)

        return M.Field(
            _name=name, _type=M.DataType.MAP, _sub_fields=tuple(sub_fields.values())
        )
