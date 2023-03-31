from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Any, Dict, List, Optional, Union, cast

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
import sqlparse
import mitzu.helper as H

import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP
import sqlalchemy.sql.sqltypes as SA_T
from sqlalchemy.orm import aliased
from sqlalchemy.sql.type_api import TypeEngine


def fix_col_index(index: int, col_name: str):
    return col_name + f"_{index}"


COLUMN_NAME_REPLACE_STR = "___"


FieldReference = Union[SA.Column, EXP.Label]
SAMPLED_SOURCE_CTE_NAME = "sampled_source"


SIMPLE_TYPE_MAPPINGS = {
    SA_T.Numeric: M.DataType.NUMBER,
    SA_T.Integer: M.DataType.NUMBER,
    SA_T.Boolean: M.DataType.BOOL,
    SA_T.DateTime: M.DataType.DATETIME,
    SA_T.Date: M.DataType.DATETIME,
    SA_T.Time: M.DataType.DATETIME,
    SA_T.String: M.DataType.STRING,
}


def format_query(raw_query: Any):
    if type(raw_query) != str:
        raw_query = str(raw_query.compile(compile_kwargs={"literal_binds": True}))

    return sqlparse.format(raw_query, reindent=True, keyword_case="upper")


@dataclass
class SegmentSubQuery:
    event_data_table: M.EventDataTable
    table_ref: SA.Table
    where_clause: Any
    event_name: Optional[str] = None
    unioned_with: Optional[SegmentSubQuery] = None

    def union_all(self, sub_query: Optional[SegmentSubQuery]) -> SegmentSubQuery:
        if sub_query is None:
            return self
        uwith = self.unioned_with
        curr = self
        while uwith is not None:
            curr = uwith
            uwith = curr.unioned_with

        curr.unioned_with = sub_query
        return self


class SQLAlchemyAdapterError(Exception):
    pass


@dataclass
class SQLAlchemyAdapter(GA.GenericDatasetAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)
        self._table_cache: Dict[str, SA.Table] = {}
        self._connection: SA.engine.Connection = None
        self._engine: SA.engine.Engine = None

    def get_event_name_field(
        self,
        ed_table: M.EventDataTable,
        sa_table: Union[SA.table, EXP.CTE] = None,
    ) -> Any:
        if ed_table.event_name_field is not None:
            return self.get_field_reference(
                ed_table.event_name_field, ed_table, sa_table
            )
        else:
            return SA.literal(ed_table.event_name_alias)

    def get_conversion_sql(self, metric: M.ConversionMetric) -> str:
        return format_query(self._get_conversion_select(metric))

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        return self.execute_query(self._get_conversion_select(metric))

    def get_segmentation_sql(self, metric: M.SegmentationMetric) -> str:
        return format_query(self._get_segmentation_select(metric))

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        return self.execute_query(self._get_segmentation_select(metric))

    def get_retention_sql(self, metric: M.RetentionMetric) -> str:
        return format_query(self._get_retention_select(metric))

    def get_retention_df(self, metric: M.RetentionMetric) -> pd.DataFrame:
        return self.execute_query(self._get_retention_select(metric))

    def map_type(self, sa_type: Any) -> M.DataType:
        for sa_t, data_type in SIMPLE_TYPE_MAPPINGS.items():
            if issubclass(type(sa_type), sa_t):
                return data_type

        raise ValueError(f"{sa_type}[{type(sa_type)}]: is not supported.")

    def keep_alive_connection(self) -> bool:
        return False

    def execute_query(self, query: Any) -> pd.DataFrame:
        engine = self.get_engine()
        try:
            if H.LOGGER.isEnabledFor(logging.DEBUG):
                H.LOGGER.debug(f"Query:\n{format_query(query)}")
            if self._connection is None:
                self._connection = engine.connect()
            cursor_result = self._connection.execute(query)
            columns = cursor_result.keys()
            fetched = cursor_result.fetchall()
            if len(fetched) > 0:
                pdf = pd.DataFrame(fetched)
                pdf.columns = columns
            else:
                pdf = pd.DataFrame(columns=columns)
            return pdf
        except Exception as exc:
            self._connection = None
            H.LOGGER.error(f"Failed Query:\n{format_query(query)}")
            raise exc
        finally:
            if not self.keep_alive_connection():
                self._connection = None

    def get_engine(self) -> SA.engine.Engine:
        con = self.project.connection
        if self._engine is None:
            if con.url is None:
                url = self._get_connection_url(con)
            else:
                url = con.url
            self._engine = SA.create_engine(url)
        return self._engine

    def get_table_by_name(self, schema: str, table_name: str) -> SA.Table:
        full_name = f"{schema}.{table_name}"
        try:
            engine = self.get_engine()
        except Exception as e:
            raise SQLAlchemyAdapterError(f"Failed to connect to {full_name}") from e

        if full_name not in self._table_cache:
            metadata_obj = SA.MetaData()
            self._table_cache[full_name] = SA.Table(
                table_name,
                metadata_obj,
                schema=schema,
                autoload_with=engine,
                autoload=True,
            )
        return self._table_cache[full_name]

    def get_table(self, event_data_table: M.EventDataTable) -> SA.Table:
        if event_data_table.schema is not None:
            schema = event_data_table.schema
        elif self.project.connection.schema is not None:
            schema = self.project.connection.schema
        else:
            raise ValueError("Event data table doesn't have schema defined")
        return self.get_table_by_name(schema, event_data_table.table_name)

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

        if field._parent is None:
            return sa_table.columns.get(field._name)
        if field._parent._type == M.DataType.MAP:
            return SA.literal_column(
                f"{sa_table.name}.{field._parent._get_name()}['{field._name}']"
            )
        else:
            return SA.literal_column(f"{sa_table.name}.{field._get_name()}")

    def _get_struct_type(self) -> TypeEngine:
        raise NotImplementedError(
            "Generic SQL Alchemy Adapter doesn't support complex types (struct, row)"
        )

    def _parse_struct_type(self, sa_type: Any, name: str, path: str) -> M.Field:
        real_struct_type = self._get_struct_type()
        if isinstance(sa_type, real_struct_type):
            row: TypeEngine = sa_type
            sub_fields: List[M.Field] = []
            for n, st in row.attr_types:
                next_path = f"{path}.{n}"
                sf = self._parse_struct_type(
                    sa_type=st,
                    name=n,
                    path=next_path,
                )
                if sf._type == M.DataType and (
                    sf._sub_fields is None or len(sf._sub_fields) == 0
                ):
                    continue
                sub_fields.append(sf)
            return M.Field(
                _name=name, _type=M.DataType.STRUCT, _sub_fields=tuple(sub_fields)
            )
        else:
            return M.Field(_name=name, _type=self.map_type(sa_type))

    def _parse_map_type(
        self,
        sa_type: Any,
        name: str,
        event_data_table: M.EventDataTable,
    ) -> M.Field:
        raise NotImplementedError(
            "Generic SQL Alchemy Adapter doesn't support map types"
        )

    def _generate_retention_series_cte(
        self, start_dt: datetime, end_dt: datetime, time_window: M.TimeWindow
    ) -> EXP.CTE:
        selects = []
        curr_dt = start_dt
        index = 0
        while curr_dt <= end_dt:
            index_sel = SA.select(columns=[SA.literal(index).label(GA.RETENTION_INDEX)])
            selects.append(index_sel)
            index += time_window.value
            curr_dt = curr_dt + time_window.to_relative_delta()
        if len(selects) > 64:  # TODO: make this come from configs
            raise ValueError(
                "Too many retention periods to caluclate try reducing the scope"
            )
        return SA.union(*selects).cte()

    def list_schemas(self) -> List[str]:
        engine = self.get_engine()
        insp = SA.inspect(engine)
        return insp.get_schema_names()

    def list_tables(self, schema: str) -> List[str]:
        engine = self.get_engine()
        insp = SA.inspect(engine)
        return insp.get_table_names(schema=schema)

    def _flatten_fields(self, fields: List[M.Field]) -> List[M.Field]:
        """Flattens the tree field structure to a list.
            All leaf-nodes in the tree will be in the result list

        Args:
            fields (List[M.Field]): Fields that might contain subfields

        Returns:
            List[M.Field]: All subfields (with correct parents)
        """
        res = []
        for f in fields:
            if f._type.is_complex():
                if f._sub_fields is not None:
                    res.extend(self._flatten_fields(list(f._sub_fields)))
            else:
                res.append(f)
        return res

    def list_fields(self, event_data_table: M.EventDataTable) -> List[M.Field]:
        table = self.get_table(event_data_table)
        field_types = table.columns
        res = []
        for field_name, field_type in field_types.items():
            if field_name in event_data_table.ignored_fields:
                continue
            data_type = self.map_type(field_type.type)
            if data_type == M.DataType.STRUCT:
                complex_field = self._parse_struct_type(
                    sa_type=field_type.type,
                    name=field_name,
                    path=field_name,
                )
                if (
                    complex_field._sub_fields is None
                    or len(complex_field._sub_fields) == 0
                ):
                    continue
                res.append(complex_field)
            if data_type == M.DataType.MAP:
                map_field = self._parse_map_type(
                    sa_type=field_type.type,
                    name=field_name,
                    event_data_table=event_data_table,
                )
                if map_field._sub_fields is None or len(map_field._sub_fields) == 0:
                    continue
                res.append(map_field)
            else:
                field = M.Field(_name=field_name, _type=data_type)
                res.append(field)
        return self._flatten_fields(res)

    def list_all_table_columns(self, schema: str, table_name: str) -> List[M.Field]:
        table = self.get_table_by_name(schema, table_name)
        field_types = table.columns
        res = []
        for field_name, field_type in field_types.items():
            data_type = self.map_type(field_type.type)
            if data_type == M.DataType.STRUCT:
                complex_field = self._parse_struct_type(
                    sa_type=field_type.type,
                    name=field_name,
                    path=field_name,
                )
                if (
                    complex_field._sub_fields is None
                    or len(complex_field._sub_fields) == 0
                ):
                    continue
                res.append(complex_field)
            else:
                field = M.Field(_name=field_name, _type=data_type)
                res.append(field)
        return res

    def get_distinct_event_names(self, event_data_table: M.EventDataTable) -> List[str]:
        cte = aliased(
            self._get_dataset_discovery_cte(event_data_table),
            alias=SAMPLED_SOURCE_CTE_NAME,
            name=SAMPLED_SOURCE_CTE_NAME,
        )
        event_name_field = self.get_event_name_field(event_data_table, cte).label(
            GA.EVENT_NAME_ALIAS_COL
        )

        result = self.execute_query(
            SA.select(
                columns=[SA.distinct(event_name_field)],
            )
        )

        return pd.DataFrame(result)[GA.EVENT_NAME_ALIAS_COL].tolist()

    def _get_timewindow(self, timegroup: M.TimeGroup) -> Any:
        return SA.text(f"interval '1 {timegroup}'")

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return field_ref + SA.text(f"interval '{timewindow.value}' {timewindow.period}")

    def _get_dynamic_datetime_interval(
        self,
        field_ref: FieldReference,
        value_field_ref: FieldReference,
        time_group: M.TimeGroup,
    ) -> Any:
        return SA.func.date_add(
            SA.text(f"'{time_group.name.lower()}'"), value_field_ref, field_ref
        )

    def _get_connection_url(self, con: M.Connection):
        user_name = "" if con.user_name is None else con.user_name
        password = "" if con.password is None else f":{con.password}"
        host_str = "" if con.host is None else str(con.host)
        if con.user_name is not None and con.host is not None:
            host_str = f"@{host_str}"
        port_str = "" if con.port is None else ":" + str(con.port)
        schema_str = "" if con.schema is None else f"/{con.schema}"
        url_params_str = "" if con.url_params is None else con.url_params
        if url_params_str != "" and url_params_str[0] != "?":
            url_params_str = "?" + url_params_str
        catalog_str = "" if con.catalog is None else f"/{con.catalog}"

        protocol = con.connection_type.get_protocol().lower()
        res = f"{protocol}://{user_name}{password}{host_str}{port_str}{catalog_str}{schema_str}{url_params_str}"
        return res

    def _get_distinct_array_agg_func(self, field_ref: FieldReference) -> Any:
        return SA.func.cast(SA.func.array_agg(SA.distinct(field_ref)), SA.JSON)

    def _column_index_support(self):
        return True

    def _get_dataset_discovery_cte(self, event_data_table: M.EventDataTable) -> EXP.CTE:
        if event_data_table.discovery_settings is None:
            raise ValueError("Missing discovery settings")

        table = self.get_table(event_data_table).alias("_evt")
        event_name_field = self.get_event_name_field(event_data_table, table)
        dt_field = self.get_field_reference(
            field=event_data_table.event_time_field,
            event_data_table=event_data_table,
            sa_table=table,
        )
        date_partition_filter = self._get_date_partition_filter(
            event_data_table,
            table,
            self.project.get_default_discovery_start_dt(),
            self.project.get_default_end_dt(),
        )

        raw_cte: EXP.CTE = SA.select(
            columns=[
                *table.columns.values(),
                (SA.cast(SA.func.random() * 1367, SA.Integer) % 100).label("__sample"),
                SA.func.row_number()
                .over(partition_by=event_name_field, order_by=SA.func.random())
                .label("rn"),
            ],
            whereclause=(
                (
                    dt_field
                    >= self._correct_timestamp(
                        self.project.get_default_discovery_start_dt()
                    )
                )
                & (
                    dt_field
                    <= self._correct_timestamp(self.project.get_default_end_dt())
                )
                & date_partition_filter
            ),
        ).cte()

        sampling_condition = SA.literal(True)
        if event_data_table.discovery_settings.lookback_days > 0:
            sampling_condition = (
                raw_cte.columns["__sample"]
                < event_data_table.discovery_settings.property_sample_rate
            ) | (
                raw_cte.columns["rn"]
                <= self.project.discovery_settings.min_property_sample_size
            )
        return SA.select(
            columns=raw_cte.columns.values(), whereclause=sampling_condition
        ).cte()

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> pd.DataFrame:
        if event_data_table.discovery_settings is None:
            raise ValueError("Missing discovery settings")

        event_specific_str = "event specific" if event_specific else "generic"
        H.LOGGER.debug(f"Discovering {event_specific_str} field enums")

        cte = aliased(
            self._get_dataset_discovery_cte(event_data_table),
            alias=SAMPLED_SOURCE_CTE_NAME,
            name=SAMPLED_SOURCE_CTE_NAME,
        )
        event_name_select_field = (
            self.get_event_name_field(event_data_table, cte).label(
                GA.EVENT_NAME_ALIAS_COL
            )
            if event_specific
            else SA.literal(M.ANY_EVENT_NAME).label(GA.EVENT_NAME_ALIAS_COL)
        )

        query = SA.select(
            group_by=(
                SA.literal(1)
                if self._column_index_support()
                else SA.text(GA.EVENT_NAME_ALIAS_COL)
            ),
            columns=[event_name_select_field]
            + [
                SA.case(
                    (
                        SA.func.count(
                            SA.distinct(self.get_field_reference(f, sa_table=cte))
                        )
                        < event_data_table.discovery_settings.max_enum_cardinality,
                        self._get_distinct_array_agg_func(
                            self.get_field_reference(f, sa_table=cte)
                        ),
                    ),
                    else_=SA.literal(None),
                ).label(f._get_name().replace(".", COLUMN_NAME_REPLACE_STR))
                for f in fields
            ],
        )
        df = self.execute_query(query)
        df = df.rename(
            # This is required for complext types, as aliasing with `.`
            # doesn't work. The `.` comes from the get _get_name()
            # This issue might appear elsewhere as well
            columns={
                k: k.replace(COLUMN_NAME_REPLACE_STR, ".") for k in list(df.columns)
            }
        )
        return df.set_index(GA.EVENT_NAME_ALIAS_COL)

    def get_field_enums(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> Dict[str, M.EventDef]:
        enums = self._get_column_values_df(
            event_data_table, fields, event_specific
        ).to_dict("index")
        res: Dict[str, M.EventDef] = {}
        for evt, values in enums.items():
            field_defs: Dict[M.Field, M.EventFieldDef] = {}
            for f in fields:
                field_values = values[f._get_name()]
                if (
                    (field_values is None)
                    or (len(field_values) == 1 and field_values[0])
                    or (len(field_values) > 1)
                ):
                    vals = [v for v in field_values if v] if field_values else None
                    field_defs[f] = M.EventFieldDef(
                        _event_name=evt,
                        _field=f,
                        _event_data_table=event_data_table,
                        _enums=vals,
                    )

            res[evt] = M.EventDef(
                _event_name=evt,
                _fields=field_defs,
                _event_data_table=event_data_table,
            )
        return res

    def _get_date_trunc(
        self, time_group: M.TimeGroup, field_ref: FieldReference
    ) -> Any:
        return SA.func.date_trunc(time_group.name, field_ref)

    def _get_simple_segment_condition(
        self, table: SA.Table, segment: M.SimpleSegment
    ) -> Any:
        left = cast(M.EventFieldDef, segment._left)
        ref = self.get_field_reference(left._field, sa_table=table)
        op = segment._operator
        if op == M.Operator.IS_NULL:
            return ref.is_(None)
        if op == M.Operator.IS_NOT_NULL:
            return ref.is_not(None)

        if segment._right is None:
            return SA.literal(True)

        if op == M.Operator.EQ:
            return ref == segment._right
        if op == M.Operator.NEQ:
            return ref != segment._right
        if op == M.Operator.GT:
            return ref > segment._right
        if op == M.Operator.LT:
            return ref < segment._right
        if op == M.Operator.GT_EQ:
            return ref >= segment._right
        if op == M.Operator.LT_EQ:
            return ref <= segment._right
        if op == M.Operator.LIKE:
            return ref.like(segment._right)
        if op == M.Operator.NOT_LIKE:
            return SA.not_(ref.like(segment._right))
        if op == M.Operator.ANY_OF:
            if len(segment._right) == 0:
                return SA.literal(True)
            return ref.in_(segment._right)
        if op == M.Operator.NONE_OF:
            if len(segment._right) == 0:
                return SA.literal(True)
            return SA.not_(ref.in_(segment._right))
        raise ValueError(f"Operator {op} is not supported by SQLAlchemy Adapter.")

    def _get_segment_sub_query(
        self, segment: M.Segment, metric: M.Metric, step: int
    ) -> SegmentSubQuery:
        if isinstance(segment, M.SimpleSegment):
            s = cast(M.SimpleSegment, segment)
            left = s._left
            edt = left._event_data_table
            table = self.get_table(edt)
            evt_name_col = self.get_event_name_field(edt)
            event_time_filter = self._get_timewindow_where_clause(
                edt, table, metric, step
            )

            event_name_filter = (
                (evt_name_col == left._event_name)
                if left._event_name != M.ANY_EVENT_NAME
                else SA.literal(True)
            )
            if s._operator is None:
                return SegmentSubQuery(
                    event_name=left._event_name,
                    event_data_table=left._event_data_table,
                    table_ref=table,
                    where_clause=(event_name_filter & event_time_filter),
                )
            else:
                return SegmentSubQuery(
                    event_name=left._event_name,
                    event_data_table=left._event_data_table,
                    table_ref=table,
                    where_clause=(
                        event_name_filter
                        & event_time_filter
                        & self._get_simple_segment_condition(table, segment)
                    ),
                )
        elif isinstance(segment, M.ComplexSegment):
            c = cast(M.ComplexSegment, segment)
            l_query = self._get_segment_sub_query(c._left, metric, step)
            r_query = self._get_segment_sub_query(c._right, metric, step)
            if c._operator == M.BinaryOperator.AND:
                if l_query.event_data_table != r_query.event_data_table:
                    raise Exception(
                        "And (&) operator can only be between the same events (e.g. page_view & page_view)"
                    )
                return SegmentSubQuery(
                    event_name=None,
                    event_data_table=l_query.event_data_table,
                    table_ref=l_query.table_ref,
                    where_clause=l_query.where_clause & r_query.where_clause,
                )
            else:
                if l_query.event_data_table == r_query.event_data_table:
                    merged = SegmentSubQuery(
                        event_data_table=l_query.event_data_table,
                        table_ref=l_query.table_ref,
                        where_clause=l_query.where_clause | r_query.where_clause,
                    )
                    merged = merged.union_all(l_query.unioned_with)
                    return merged.union_all(r_query.unioned_with)
                else:
                    return l_query.union_all(r_query)
        else:
            raise ValueError(f"Segment of type {type(segment)} is not supported.")

    def _has_edt_event_field(
        self,
        group_field: M.EventFieldDef,
        ed_table: M.EventDataTable,
    ) -> bool:
        event_name = group_field._event_name
        field = group_field._field
        dd: Optional[M.DiscoveredProject] = self.project._discovered_project.get_value()
        if dd is None:
            raise Exception("No DiscoveredProject was provided to SQLAlchemy Adapter.")
        events = dd.definitions.get(ed_table)
        if events is not None:
            event_def_reference = events.get(event_name)
            if event_def_reference is not None:
                event_def = event_def_reference.get_value_if_exists()
                for edt_evt_field in event_def._fields:
                    if edt_evt_field == field:
                        return True
        return False

    def _find_group_by_field_ref(
        self,
        group_field: M.EventFieldDef,
        sa_table: SA.Table,
        ed_table: M.EventDataTable,
    ):
        field = group_field._field
        if field._parent is None:
            columns = sa_table.columns
            if field._name in columns:
                return self.get_field_reference(field, sa_table=sa_table)
        elif self._has_edt_event_field(group_field, ed_table):
            return self.get_field_reference(field, sa_table=sa_table)
        return SA.literal(None)

    def _get_segment_sub_query_cte(
        self,
        sub_query: SegmentSubQuery,
        group_field: Optional[M.EventFieldDef] = None,
        resolution: M.Resolution = M.Resolution.EVERY_EVENT,
    ) -> EXP.CTE:
        selects = []
        while True:
            ed_table = sub_query.event_data_table

            group_by_col = SA.literal(None)
            if group_field is not None:
                group_by_col = self._find_group_by_field_ref(
                    group_field, sub_query.table_ref, ed_table
                )

            datetime_col = self.get_field_reference(ed_table.event_time_field, ed_table)
            resolution_tg = resolution.get_time_group()
            if resolution_tg is not None:
                datetime_col = self._get_date_trunc(
                    field_ref=datetime_col,
                    time_group=resolution_tg,
                )

            select = SA.select(
                columns=[
                    self.get_field_reference(ed_table.user_id_field, ed_table).label(
                        GA.CTE_USER_ID_ALIAS_COL
                    ),
                    datetime_col.label(GA.CTE_DATETIME_COL),
                    group_by_col.label(GA.CTE_GROUP_COL),
                ],
                whereclause=(sub_query.where_clause),
            )
            if resolution != M.Resolution.EVERY_EVENT:
                select = select.distinct()

            selects.append(select)
            if sub_query.unioned_with is not None:
                sub_query = sub_query.unioned_with
            else:
                break

        return SA.union_all(*selects).cte()

    def _correct_timestamp(self, dt: datetime) -> Any:
        return dt

    def _get_date_partition_filter(
        self,
        edt: M.EventDataTable,
        table: SA.Table,
        start_dt: datetime,
        end_dt: datetime,
    ):
        if edt.date_partition_field is not None:
            dt_part = SA.func.date(
                self.get_field_reference(edt.date_partition_field, edt, table)
            )
            return (dt_part >= SA.func.date(start_dt.date())) & (
                dt_part <= SA.func.date(end_dt.date())
            )
        else:
            return SA.literal(True)

    def _get_timewindow_where_clause(
        self, edt: M.EventDataTable, table: SA.Table, metric: M.Metric, step: int
    ) -> Any:
        evt_time_col = self.get_field_reference(edt.event_time_field, edt)
        start_date = metric._start_dt
        end_date = metric._end_dt

        if step > 0:
            if isinstance(metric, M.ConversionMetric):
                end_date = end_date + metric._conv_window.to_relative_delta()
            elif isinstance(metric, M.RetentionMetric):
                end_date = end_date + metric._retention_window.to_relative_delta()

        if metric._resolution == M.Resolution.ONE_USER_EVENT_PER_DAY:
            start_date = start_date.replace(hour=0, minute=0, second=0)
        elif metric._resolution == M.Resolution.ONE_USER_EVENT_PER_HOUR:
            start_date = start_date.replace(minute=0, second=0)
        elif metric._resolution == M.Resolution.ONE_USER_EVENT_PER_MINUTE:
            start_date = start_date.replace(second=0)

        start_date = start_date.replace(microsecond=0)
        end_date = end_date.replace(microsecond=0)
        event_time_filter = (evt_time_col >= self._correct_timestamp(start_date)) & (
            evt_time_col <= self._correct_timestamp(end_date)
        )
        date_partition_filter = self._get_date_partition_filter(
            edt, table, start_date, end_date
        )
        return event_time_filter & date_partition_filter

    def _get_seg_aggregation(self, metric: M.Metric, cte: EXP.CTE) -> Any:
        at = metric._agg_type
        if at == M.AggType.COUNT_EVENTS:
            return SA.func.count(cte.columns.get(GA.CTE_USER_ID_ALIAS_COL))
        elif at == M.AggType.COUNT_UNIQUE_USERS:
            return SA.func.count(cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct())
        else:
            raise ValueError(
                f"Aggregation type {at.name} is not supported for segmentation"
            )

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:
        at = metric._agg_type
        if at == M.AggType.CONVERSION:
            return (
                SA.func.count(cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct())
                * 100.0
                / SA.func.count(
                    first_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
                )
            )
        elif at == M.AggType.PERCENTILE_TIME_TO_CONV:
            raise NotImplementedError(
                "Sql Alchemy adapter doesn't support percentile calculation"
            )
        else:
            raise ValueError(f"Aggregation type {at} is not supported for conversion")

    def _get_segmentation_select(self, metric: M.SegmentationMetric) -> Any:
        sub_query = self._get_segment_sub_query(metric._segment, metric, step=0)
        cte: EXP.CTE = aliased(
            self._get_segment_sub_query_cte(sub_query, metric._group_by)
        )

        evt_time_group = (
            self._get_date_trunc(
                field_ref=cte.columns.get(GA.CTE_DATETIME_COL),
                time_group=metric._time_group,
            )
            if metric._time_group != M.TimeGroup.TOTAL
            else SA.literal(None)
        )

        group_by = (
            cte.columns.get(GA.CTE_GROUP_COL)
            if metric._group_by is not None
            else SA.literal(None)
        )

        return SA.select(
            columns=[
                evt_time_group.label(GA.DATETIME_COL),
                group_by.label(GA.GROUP_COL),
                self._get_seg_aggregation(metric, cte).label(GA.AGG_VALUE_COL),
            ],
            group_by=(
                [SA.literal(1), SA.literal(2)]
                if self._column_index_support()
                else [SA.text(GA.DATETIME_COL), SA.text(GA.GROUP_COL)]
            ),
        )

    def _get_conversion_select(self, metric: M.ConversionMetric) -> Any:
        first_segment = metric._conversion._segments[0]
        first_cte = self._get_segment_sub_query_cte(
            self._get_segment_sub_query(first_segment, metric, step=0),
            metric._group_by,
            metric._resolution,
        )
        first_group_by = (
            first_cte.columns.get(GA.CTE_GROUP_COL)
            if metric._group_by is not None
            else SA.literal(None)
        )

        time_group = metric._time_group
        if time_group != M.TimeGroup.TOTAL:
            first_evt_time_group = self._get_date_trunc(
                field_ref=first_cte.columns.get(GA.CTE_DATETIME_COL),
                time_group=time_group,
            )
        else:
            first_evt_time_group = SA.literal(None)

        other_segments = metric._conversion._segments[1:]

        steps = [first_cte]
        other_selects = []
        joined_source = first_cte
        for i, seg in enumerate(other_segments):
            prev_table = steps[i]
            prev_cols = prev_table.columns
            curr_cte = self._get_segment_sub_query_cte(
                self._get_segment_sub_query(seg, metric, step=i + 1),
                resolution=metric._resolution,
            )
            curr_cols = curr_cte.columns
            curr_used_id_col = curr_cols.get(GA.CTE_USER_ID_ALIAS_COL)

            steps.append(curr_cte)
            other_selects.extend(
                [
                    SA.func.count(
                        curr_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
                    ).label(fix_col_index(i + 2, GA.USER_COUNT_COL)),
                    self._get_conv_aggregation(metric, curr_cte, first_cte).label(
                        fix_col_index(i + 2, GA.AGG_VALUE_COL)
                    ),
                ]
            )
            joined_source = joined_source.join(
                curr_cte,
                (
                    (prev_cols.get(GA.CTE_USER_ID_ALIAS_COL) == curr_used_id_col)
                    & (
                        curr_cols.get(GA.CTE_DATETIME_COL)
                        > prev_cols.get(GA.CTE_DATETIME_COL)
                    )
                    & (
                        curr_cols.get(GA.CTE_DATETIME_COL)
                        <= self._get_datetime_interval(
                            first_cte.columns.get(GA.CTE_DATETIME_COL),
                            metric._conv_window,
                        )
                    )
                ),
                isouter=True,
            )

        columns = [
            first_evt_time_group.label(GA.DATETIME_COL),
            first_group_by.label(GA.GROUP_COL),
            SA.func.count(
                first_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
            ).label(fix_col_index(1, GA.USER_COUNT_COL)),
            self._get_conv_aggregation(metric, first_cte, first_cte).label(
                fix_col_index(1, GA.AGG_VALUE_COL)
            ),
        ]
        columns.extend(other_selects)
        return SA.select(
            columns=columns,
            group_by=(
                [SA.literal(1), SA.literal(2)]
                if self._column_index_support()
                else [SA.text(GA.DATETIME_COL), SA.text(GA.GROUP_COL)]
            ),
        ).select_from(joined_source)

    def _get_retention_select(self, metric: M.RetentionMetric) -> Any:
        initial_cte = self._get_segment_sub_query_cte(
            self._get_segment_sub_query(metric._initial_segment, metric, step=0),
            metric._group_by,
            metric._resolution,
        )

        retention_index_cte = self._generate_retention_series_cte(
            metric._start_dt, metric._end_dt, metric._retention_window
        ).alias("ret_indeces")

        initial_group_by = (
            initial_cte.columns.get(GA.CTE_GROUP_COL)
            if metric._group_by is not None
            else SA.literal(None)
        )

        time_group = (
            self._get_date_trunc(
                metric._time_group, initial_cte.columns.get(GA.CTE_DATETIME_COL)
            )
            if metric._time_group != M.TimeGroup.TOTAL
            else SA.literal(None)
        )

        retaining_cte = self._get_segment_sub_query_cte(
            self._get_segment_sub_query(metric._retaining_segment, metric, step=1),
            resolution=metric._resolution,
        )

        retention_interval_func = self._get_dynamic_datetime_interval(
            field_ref=initial_cte.columns.get(GA.CTE_DATETIME_COL),
            value_field_ref=retention_index_cte.columns.get(GA.RETENTION_INDEX),
            time_group=metric._retention_window.period,
        )

        joined_source = initial_cte.join(retention_index_cte, True).join(
            retaining_cte,
            (
                (
                    initial_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL)
                    == retaining_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL)
                )
                & (
                    self._get_datetime_column(retaining_cte, GA.CTE_DATETIME_COL)
                    > retention_interval_func
                )
                & (
                    self._get_datetime_column(retaining_cte, GA.CTE_DATETIME_COL)
                    <= self._get_datetime_interval(
                        retention_interval_func,
                        metric._retention_window,
                    )
                )
            ),
            isouter=True,
        )

        columns = [
            time_group.label(GA.DATETIME_COL),
            initial_group_by.label(GA.GROUP_COL),
            retention_index_cte.columns.get(GA.RETENTION_INDEX),
            SA.func.count(
                initial_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
            ).label(fix_col_index(1, GA.USER_COUNT_COL)),
            SA.func.count(
                retaining_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
            ).label(fix_col_index(2, GA.USER_COUNT_COL)),
            (
                SA.func.count(
                    retaining_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
                )
                * 100.0
                / SA.func.count(
                    initial_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
                )
            ).label(GA.AGG_VALUE_COL),
        ]

        return SA.select(
            columns=columns,
            group_by=(
                [SA.literal(1), SA.literal(2), SA.literal(3)]
                if self._column_index_support()
                else [
                    SA.text(GA.DATETIME_COL),
                    SA.text(GA.GROUP_COL),
                    SA.text(GA.RETENTION_INDEX),
                ]
            ),
        ).select_from(joined_source)

    def _get_datetime_column(self, cte: Any, name: str) -> Any:
        return cte.columns.get(name)

    def test_connection(self):
        self.execute_query(
            SA.select(columns=[SA.literal(True).label("test_connection")])
        )

    def stop_current_execution(self):
        if self._connection is not None:
            self._connection.connection.close()
