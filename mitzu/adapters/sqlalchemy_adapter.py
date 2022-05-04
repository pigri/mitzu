from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, cast, Optional
import mitzu.adapters.generic_adapter as GA
import mitzu.common.model as M
import pandas as pd  # type: ignore
import sqlalchemy as SA  # type: ignore
from sqlalchemy.orm import aliased  # type:ignore
from sqlalchemy.sql.expression import CTE, Select  # type:ignore
from sql_formatter.core import format_sql  # type: ignore


@dataclass
class SegmentSubQuery:
    event_name: str
    event_data_table: M.EventDataTable
    table_ref: SA.Table
    where_clause: Any
    _unioned_with: Optional[SegmentSubQuery] = None

    def union_all(self, sub_query: SegmentSubQuery):
        u = self._unioned_with
        curr = self
        while u is not None:
            curr = u
            u = curr._unioned_with
        curr._unioned_with = sub_query


class SQLAlchemyAdapter(GA.GenericDatasetAdapter):
    _table_cache: Dict[M.EventDataTable, SA.Table] = {}
    _engine: Any

    def __init__(self, source: M.EventDataSource):
        super().__init__(source)
        self._table = None
        self._engine = None

    def get_conversion_sql(self, metric: M.ConversionMetric) -> str:
        return format_sql(
            str(
                self._get_conversion_select(metric).compile(
                    compile_kwargs={"literal_binds": True}
                )
            )
        )

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        return self.execute_query(self._get_conversion_select(metric))

    def get_segmentation_sql(self, metric: M.SegmentationMetric) -> str:
        return format_sql(
            str(
                self._get_segmentation_select(metric).compile(
                    compile_kwargs={"literal_binds": True}
                )
            )
        )

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        return self.execute_query(self._get_segmentation_select(metric))

    def map_type(self, sa_type: Any) -> M.DataType:
        if isinstance(sa_type, SA.Integer):
            return M.DataType.NUMBER
        if isinstance(sa_type, SA.Float):
            return M.DataType.NUMBER
        if isinstance(sa_type, SA.Text):
            return M.DataType.STRING
        if isinstance(sa_type, SA.String):
            return M.DataType.STRING
        if isinstance(sa_type, SA.DateTime):
            return M.DataType.DATETIME
        raise ValueError(f"{sa_type}[{type(sa_type)}]: is not supported.")

    def execute_query(self, query: Any) -> pd.DataFrame:
        engine = self.get_engine()
        try:
            result = engine.execute(query)
            columns = result.keys()
            fetched = result.fetchall()
            if len(fetched) > 0:
                pdf = pd.DataFrame(fetched)
                pdf.columns = columns
            else:
                pdf = pd.DataFrame(columns=columns)
            return pdf
        except Exception as exc:
            print("Failed Query:")
            if type(query) == str:
                print(format_sql(str(query)))
            else:
                print(
                    format_sql(
                        str(query.compile(compile_kwargs={"literal_binds": True}))
                    )
                )
            raise exc

    def get_engine(self) -> Any:
        con = self.source.connection
        if self._engine is None:
            if self.source.connection.ssh_tunnel_forwarder is not None:
                self._create_ssh_tunnel()

            if con.url is None:
                url = self._get_connection_url(con)
            else:
                url = con.url
            self._engine = SA.create_engine(url)
        return self._engine

    def get_table(
        self, event_data_table: M.EventDataTable, create_alias: bool = True
    ) -> SA.Table:
        if event_data_table not in self._table_cache:
            engine = self.get_engine()
            metadata_obj = SA.MetaData()
            self._table_cache[event_data_table] = SA.Table(
                event_data_table.table_name,
                metadata_obj,
                autoload_with=engine,
                autoload=True,
            )
        cached_table = self._table_cache[event_data_table]
        return aliased(cached_table) if create_alias else cached_table

    def validate_source(self):
        table = self.get_table()
        if (
            self.source.user_id_field not in table.columns
            or self.source.event_time_field not in table.columns
            or self.source.event_name_field not in table.columns
        ):
            raise Exception("Table doesn't contain all essential columns.")

    def list_fields(self, event_data_table: M.EventDataTable) -> List[M.Field]:
        table = self.get_table(event_data_table)
        field_types = table.columns.items()
        return [M.Field(_name=k, _type=self.map_type(v.type)) for k, v in field_types]

    def get_distinct_event_names(self, event_data_table: M.EventDataTable) -> List[str]:
        table = self.get_table(event_data_table)
        result = self.execute_query(
            SA.select(
                [SA.distinct(table.columns.get(event_data_table.event_name_field))]
            )
        )
        return pd.DataFrame(result)[event_data_table.event_name_field].tolist()

    def _get_datetime_interval(
        self, table_column: SA.Column, timewindow: M.TimeWindow
    ) -> Any:
        return table_column + SA.text(
            f"interval '{timewindow.value}' {timewindow.period}"
        )

    def _get_connection_url(self, con: M.Connection):
        credentials = (
            "" if con.user_name is None else f"{con.user_name}:{con.password}@"
        )
        host_str = "" if con.host is None else str(con.host)
        port_str = "" if con.port is None else ":" + str(con.port)
        schema_str = "" if con.schema is None else f"/{con.schema}"
        url_params_str = "" if con.url_params is None else con.url_params
        if url_params_str != "" and url_params_str[0] != "?":
            url_params_str = "?" + url_params_str

        protocol = con.connection_type.value.lower()
        return f"{protocol}://{credentials}{host_str}{port_str}{schema_str}{url_params_str}"

    def _get_distinct_array_agg_func(self, column: SA.Column) -> Any:
        return SA.func.array_agg(column.distinct())

    def _column_index_support(self):
        return True

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> pd.DataFrame:
        source = self.source
        columns = self.get_table(event_data_table).columns
        event_name_field = event_data_table.event_name_field
        event_name_select_field = (
            columns.get(event_name_field).label(GA.EVENT_NAME_ALIAS_COL)
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
                        SA.func.count(columns.get(f._name).distinct())
                        < source.max_enum_cardinality,
                        self._get_distinct_array_agg_func(columns.get(f._name)),
                    ),
                    else_=SA.literal(None),
                ).label(f._name)
                for f in fields
                if f._name != event_name_field
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
            res[evt] = M.EventDef(
                _event_name=evt,
                _fields={
                    f: M.EventFieldDef(
                        _event_name=evt,
                        _field=f,
                        _source=self.source,
                        _event_data_table=event_data_table,
                        _enums=values[f._name],
                    )
                    for f in fields
                    if f._name != event_data_table.event_name_field
                },
                _source=self.source,
                _event_data_table=event_data_table,
            )
        return res

    def _get_date_trunc(self, time_group: M.TimeGroup, table_column: SA.Column) -> Any:
        return SA.func.date_trunc(time_group.name, table_column)

    def _get_simple_segment_condition(
        self, table: SA.Table, segment: M.SimpleSegment
    ) -> Any:
        left = cast(M.EventFieldDef, segment._left)
        field = table.columns.get(left._field._name)
        op = segment._operator
        if op == M.Operator.EQ:
            return field == segment._right
        if op == M.Operator.NEQ:
            return field != segment._right
        if op == M.Operator.GT:
            return field > segment._right
        if op == M.Operator.LT:
            return field < segment._right
        if op == M.Operator.GT_EQ:
            return field >= segment._right
        if op == M.Operator.LT_EQ:
            return field <= segment._right
        if op == M.Operator.LIKE:
            return field.like(segment._right)
        if op == M.Operator.NOT_LIKE:
            return SA.not_(field.like(segment._right))
        if op == M.Operator.ANY_OF:
            return field.in_(segment._right)
        if op == M.Operator.NONE_OF:
            return SA.not_(field.in_(segment._right))
        if op == M.Operator.IS_NULL:
            return field is None
        if op == M.Operator.IS_NOT_NULL:
            return field is not None
        raise ValueError(f"Operator {op} is not supported by SQLAlchemy Adapter.")

    def _get_segment_sub_query(self, segment: M.Segment) -> SegmentSubQuery:
        if isinstance(segment, M.SimpleSegment):
            s = cast(M.SimpleSegment, segment)
            left = s._left
            table = self.get_table(left._event_data_table, create_alias=False)
            evt_name_col = table.columns.get(left._event_data_table.event_name_field)
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
                    where_clause=event_name_filter,
                )
            else:
                return SegmentSubQuery(
                    event_name=left._event_name,
                    event_data_table=left._event_data_table,
                    table_ref=table,
                    where_clause=(
                        (event_name_filter)
                        & self._get_simple_segment_condition(table, segment)
                    ),
                )
        elif isinstance(segment, M.ComplexSegment):
            c = cast(M.ComplexSegment, segment)
            l_query = self._get_segment_sub_query(c._left)
            r_query = self._get_segment_sub_query(c._right)
            if c._operator == M.BinaryOperator.AND:
                if l_query.event_data_table != r_query.event_data_table:
                    raise Exception(
                        "And (&) or OR (|) operators can only be between the same events (e.g. page_view & page_view)"
                    )
                return SegmentSubQuery(
                    event_name=l_query.event_name,
                    event_data_table=l_query.event_data_table,
                    table_ref=l_query.table_ref,
                    where_clause=l_query.where_clause & r_query.where_clause,
                )
            else:
                if l_query.event_data_table == r_query.event_data_table:
                    return SegmentSubQuery(
                        event_name=l_query.event_name,
                        event_data_table=l_query.event_data_table,
                        table_ref=l_query.table_ref,
                        where_clause=l_query.where_clause | r_query.where_clause,
                    )
                else:
                    l_query.union_all(r_query)
                    return l_query
        else:
            # TODO add more validations
            raise ValueError(f"Segment of type {type(segment)} is not supported.")

    def _get_segment_sub_query_cte(
        self, sub_query: SegmentSubQuery, group_field: Optional[M.EventFieldDef] = None
    ) -> CTE:
        res_select: Select = None
        while True:
            ed_table = sub_query.event_data_table
            columns = sub_query.table_ref.columns
            group_by_col = (
                SA.literal(None)
                if group_field is None
                else columns.get(group_field._field._name)
            )
            select = SA.select(
                columns=[
                    columns.get(ed_table.user_id_field).label(GA.CTE_USER_ID_ALIAS_COL),
                    columns.get(ed_table.event_time_field).label(GA.CTE_DATETIME_COL),
                    group_by_col.label(GA.CTE_GROUP_COL),
                ],
                whereclause=(sub_query.where_clause),
            )
            if res_select is None:
                res_select = select
            else:
                res_select = res_select.union_all(select)
            if sub_query._unioned_with is not None:
                sub_query = sub_query._unioned_with
            else:
                break

        return res_select.cte()

    def _get_timewindow_where_clause(self, cte: CTE, metric: M.Metric) -> Any:
        start_date = metric._start_dt
        end_date = metric._end_dt

        evt_time_col = cte.columns.get(GA.CTE_DATETIME_COL)
        return (evt_time_col >= start_date) & (evt_time_col <= end_date)

    def _get_segmentation_select(self, metric: M.SegmentationMetric) -> Any:
        sub_query = self._get_segment_sub_query(metric._segment)
        cte: CTE = aliased(self._get_segment_sub_query_cte(sub_query, metric._group_by))

        evt_time_group = (
            self._get_date_trunc(
                table_column=cte.columns.get(GA.CTE_DATETIME_COL),
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
                SA.func.count(
                    cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
                ).label(GA.USER_COUNT_COL),
                SA.func.count(cte.columns.get(GA.CTE_USER_ID_ALIAS_COL)).label(
                    GA.EVENT_COUNT_COL
                ),
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
            self._get_segment_sub_query(first_segment), metric._group_by
        )
        first_group_by = (
            first_cte.columns.get(GA.CTE_GROUP_COL)
            if metric._group_by is not None
            else SA.literal(None)
        )

        time_group = metric._time_group
        if time_group != M.TimeGroup.TOTAL:
            first_evt_time_group = self._get_date_trunc(
                table_column=first_cte.columns.get(GA.CTE_DATETIME_COL),
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
            curr_cte = self._get_segment_sub_query_cte(self._get_segment_sub_query(seg))
            curr_cols = curr_cte.columns
            curr_used_id_col = curr_cols.get(GA.CTE_USER_ID_ALIAS_COL)

            steps.append(curr_cte)
            other_selects.extend(
                [
                    SA.func.count(curr_used_id_col.distinct()).label(
                        self._fix_col_index(i + 2, GA.USER_COUNT_COL)
                    ),
                    SA.func.count(curr_used_id_col).label(
                        self._fix_col_index(i + 2, GA.EVENT_COUNT_COL)
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
            (
                SA.func.count(
                    steps[len(steps) - 1]
                    .columns.get(GA.CTE_USER_ID_ALIAS_COL)
                    .distinct()
                )
                * 1.0
                / SA.func.count(
                    first_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL).distinct()
                )
            ).label(GA.CVR_COL),
            SA.func.count(first_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL)).label(
                self._fix_col_index(1, GA.USER_COUNT_COL)
            ),
            SA.func.count(first_cte.columns.get(GA.CTE_USER_ID_ALIAS_COL)).label(
                self._fix_col_index(1, GA.EVENT_COUNT_COL)
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
