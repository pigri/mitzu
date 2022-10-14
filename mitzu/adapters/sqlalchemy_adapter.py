from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, cast

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import pandas as pd
import sqlparse

import sqlalchemy as SA
import sqlalchemy.sql.expression as EXP
import sqlalchemy.sql.sqltypes as SA_T
from sqlalchemy.orm import aliased


def fix_col_index(index: int, col_name: str):
    return col_name + f"_{index}"


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

    def map_type(self, sa_type: Any) -> M.DataType:
        for sa_t, data_type in SIMPLE_TYPE_MAPPINGS.items():
            if issubclass(type(sa_type), sa_t):
                return data_type

        raise ValueError(f"{sa_type}[{type(sa_type)}]: is not supported.")

    def execute_query(self, query: Any) -> pd.DataFrame:
        engine = self.get_engine()
        try:
            conn = engine.connect()
            self._connection = conn
            cursor_result = conn.execute(query)
            columns = cursor_result.keys()
            fetched = cursor_result.fetchall()
            if len(fetched) > 0:
                pdf = pd.DataFrame(fetched)
                pdf.columns = columns
            else:
                pdf = pd.DataFrame(columns=columns)
            return pdf
        except Exception as exc:
            print("Failed Query:")
            print(format_query(query))
            raise exc
        finally:
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

    def get_table(self, event_data_table: M.EventDataTable) -> SA.Table:
        full_name = event_data_table.get_full_name()
        if full_name not in self._table_cache:
            # print(f"Table {full_name} not in Cache")
            engine = self.get_engine()
            metadata_obj = SA.MetaData()

            if event_data_table.schema is not None:
                schema = event_data_table.schema
            elif self.project.connection.schema is not None:
                schema = self.project.connection.schema
            else:
                schema = None
            self._table_cache[full_name] = SA.Table(
                event_data_table.table_name,
                metadata_obj,
                schema=schema,
                autoload_with=engine,
                autoload=True,
            )
        return self._table_cache[full_name]

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

        return SA.literal_column(f"{sa_table.name}.{field._get_name()}")

    def _parse_complex_type(
        self, sa_type: Any, name: str, event_data_table: M.EventDataTable, path: str
    ) -> M.Field:
        raise NotImplementedError(
            "Generic SQL Alchemy Adapter doesn't support complex types (struct, row)"
        )

    def _parse_map_type(
        self,
        sa_type: Any,
        name: str,
        event_data_table: M.EventDataTable,
    ) -> M.Field:
        raise NotImplementedError(
            "Generic SQL Alchemy Adapter doesn't support map types"
        )

    def list_fields(self, event_data_table: M.EventDataTable) -> List[M.Field]:
        table = self.get_table(event_data_table)
        field_types = table.columns
        res = []
        for field_name, field_type in field_types.items():
            if field_name in event_data_table.ignored_fields:
                continue
            data_type = self.map_type(field_type.type)
            if data_type == M.DataType.STRUCT:
                complex_field = self._parse_complex_type(
                    sa_type=field_type.type,
                    name=field_name,
                    event_data_table=event_data_table,
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

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return field_ref + SA.text(f"interval '{timewindow.value}' {timewindow.period}")

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

        protocol = con.connection_type.value.lower()
        res = f"{protocol}://{user_name}{password}{host_str}{port_str}{catalog_str}{schema_str}{url_params_str}"
        return res

    def _get_distinct_array_agg_func(self, field_ref: FieldReference) -> Any:
        return SA.func.array_agg(SA.distinct(field_ref))

    def _column_index_support(self):
        return True

    def _get_dataset_discovery_cte(self, event_data_table: M.EventDataTable) -> EXP.CTE:
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

        return SA.select(
            columns=raw_cte.columns.values(),
            whereclause=(
                raw_cte.columns["rn"] < self.project.default_property_sample_size
            ),
        ).cte()

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> pd.DataFrame:
        event_specifig_str = "event specific" if event_specific else "generic"
        print(f"Discovering {event_specifig_str} field enums")

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
                        < self.project.max_enum_cardinality,
                        self._get_distinct_array_agg_func(
                            self.get_field_reference(f, sa_table=cte)
                        ),
                    ),
                    else_=SA.literal(None),
                ).label(f._get_name())
                for f in fields
            ],
        )
        df = self.execute_query(query)
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
        res = {}
        for evt, values in enums.items():
            for f in fields:
                field_name = f._get_name()
                if (
                    values[field_name] is not None
                    and len(values[field_name]) == 1
                    and values[field_name][0] is None
                ):
                    values[field_name] = []

            res[evt] = M.EventDef(
                _event_name=evt,
                _fields={
                    f: M.EventFieldDef(
                        _event_name=evt,
                        _field=f,
                        _project=self.project,
                        _event_data_table=event_data_table,
                        _enums=values[f._get_name()],
                    )
                    for f in fields
                    # We return NONE if the cardinality is above the max_enum_cardinality for a field.
                    # This is Needed as the NULL type will be accepted for every case-when as a return statement.
                    # Empty list are the result of field not having any value
                    if values[f._get_name()] is None or len(values[f._get_name()]) > 0
                },
                _project=self.project,
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

    def _get_segment_sub_query(
        self, segment: M.Segment, metric: M.Metric
    ) -> SegmentSubQuery:
        if isinstance(segment, M.SimpleSegment):
            s = cast(M.SimpleSegment, segment)
            left = s._left
            edt = left._event_data_table
            table = self.get_table(edt)
            evt_name_col = self.get_event_name_field(edt)
            end_dt_partition = (
                metric._end_dt + metric._conv_window.to_relative_delta()
                if isinstance(metric, M.ConversionMetric)
                else metric._end_dt
            )
            date_partition_filter = self._get_date_partition_filter(
                edt, table, metric._start_dt, end_dt_partition
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
                    where_clause=(event_name_filter & date_partition_filter),
                )
            else:
                return SegmentSubQuery(
                    event_name=left._event_name,
                    event_data_table=left._event_data_table,
                    table_ref=table,
                    where_clause=(
                        event_name_filter
                        & date_partition_filter
                        & self._get_simple_segment_condition(table, segment)
                    ),
                )
        elif isinstance(segment, M.ComplexSegment):
            c = cast(M.ComplexSegment, segment)
            l_query = self._get_segment_sub_query(c._left, metric)
            r_query = self._get_segment_sub_query(c._right, metric)
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
            event_def = events.get(event_name)
            if event_def is not None:
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
        self, sub_query: SegmentSubQuery, group_field: Optional[M.EventFieldDef] = None
    ) -> EXP.CTE:
        selects = []
        while True:
            ed_table = sub_query.event_data_table

            group_by_col = SA.literal(None)
            if group_field is not None:
                group_by_col = self._find_group_by_field_ref(
                    group_field,
                    sub_query.table_ref,
                    ed_table,
                )

            select = SA.select(
                columns=[
                    self.get_field_reference(ed_table.user_id_field, ed_table).label(
                        GA.CTE_USER_ID_ALIAS_COL
                    ),
                    self.get_field_reference(ed_table.event_time_field, ed_table).label(
                        GA.CTE_DATETIME_COL
                    ),
                    group_by_col.label(GA.CTE_GROUP_COL),
                ],
                whereclause=(sub_query.where_clause),
            )
            selects.append(select)
            if sub_query.unioned_with is not None:
                sub_query = sub_query.unioned_with
            else:
                break

        return SA.union_all(*selects).cte()

    def _correct_timestamp(self, dt: datetime) -> Any:
        return dt

    def _get_timewindow_where_clause(self, cte: EXP.CTE, metric: M.Metric) -> Any:
        start_date = metric._start_dt
        end_date = metric._end_dt

        evt_time_col = cte.columns.get(GA.CTE_DATETIME_COL)
        return (evt_time_col >= self._correct_timestamp(start_date)) & (
            evt_time_col <= self._correct_timestamp(end_date)
        )

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
        sub_query = self._get_segment_sub_query(metric._segment, metric)
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
            whereclause=(self._get_timewindow_where_clause(cte, metric)),
            group_by=(
                [SA.literal(1), SA.literal(2)]
                if self._column_index_support()
                else [SA.text(GA.DATETIME_COL), SA.text(GA.GROUP_COL)]
            ),
        )

    def _get_conversion_select(self, metric: M.ConversionMetric) -> Any:
        first_segment = metric._conversion._segments[0]
        first_cte = self._get_segment_sub_query_cte(
            self._get_segment_sub_query(first_segment, metric), metric._group_by
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
                self._get_segment_sub_query(seg, metric)
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
            whereclause=(self._get_timewindow_where_clause(first_cte, metric)),
            group_by=(
                [SA.literal(1), SA.literal(2)]
                if self._column_index_support()
                else [SA.text(GA.DATETIME_COL), SA.text(GA.GROUP_COL)]
            ),
        ).select_from(joined_source)

    def test_connection(self):
        self.execute_query(
            SA.select(columns=[SA.literal(True).label("test_connection")])
        )

    def stop_current_execution(self):

        if self._connection is not None:
            print("Stopping transaction")

            self._connection.connection.close()
