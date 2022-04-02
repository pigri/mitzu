from __future__ import annotations
from typing import Any, Dict, List, cast
import mitzu.adapters.generic_adapter as GA
import mitzu.common.model as M
import pandas as pd  # type: ignore
import sqlalchemy as SA  # type: ignore
from sqlalchemy.orm import aliased  # type:ignore
from sql_formatter.core import format_sql  # type: ignore


class SQLAlchemyAdapter(GA.GenericDatasetAdapter):
    _table: SA.Table
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
            if con.url is None:
                url = self._get_connection_url(con)
            else:
                url = con.url
            self._engine = SA.create_engine(url)
        return self._engine

    def get_table(self) -> SA.Table:
        if self._table is None:
            engine = self.get_engine()
            metadata_obj = SA.MetaData()
            self._table = SA.Table(
                self.source.table_name,
                metadata_obj,
                autoload_with=engine,
                autoload=True,
            )
        return aliased(self._table)

    def validate_source(self):
        table = self.get_table()
        if (
            self.source.user_id_field not in table.columns
            or self.source.event_time_field not in table.columns
            or self.source.event_name_field not in table.columns
        ):
            raise Exception("Table doesn't contain all essential columns.")

    def list_fields(self) -> List[M.Field]:
        table = self.get_table()
        field_types = table.columns.items()
        return [M.Field(_name=k, _type=self.map_type(v.type)) for k, v in field_types]

    def get_map_field_keys(
        self, map_field: M.Field, event_specific: bool
    ) -> Dict[str, List[M.Field]]:
        raise NotImplementedError("Map fields are not yet supported")

    def get_distinct_event_names(self) -> List[str]:
        table = self.get_table()
        result = self.execute_query(
            SA.select([SA.distinct(table.columns.get(self.source.event_name_field))])
        )
        return pd.DataFrame(result)[self.source.event_name_field].tolist()

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
        protocol = con.connection_type.value.lower()
        return f"{protocol}://{credentials}{host_str}{port_str}{schema_str}{url_params_str}"

    def _get_distinct_array_agg_func(self, column: SA.Column) -> Any:
        return SA.func.array_agg(column.distinct())

    def _column_index_support(self):
        return True

    def _get_column_values_df(
        self, fields: List[M.Field], event_specific: bool
    ) -> pd.DataFrame:
        source = self.source
        columns = self.get_table().columns
        event_name_field = source.event_name_field
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
        self, fields: List[M.Field], event_specific: bool
    ) -> Dict[str, M.EventDef]:
        enums = self._get_column_values_df(fields, event_specific).to_dict("index")
        res = {}
        for evt, values in enums.items():
            res[evt] = M.EventDef(
                _event_name=evt,
                _fields={
                    f: M.EventFieldDef(
                        _event_name=evt,
                        _field=f,
                        _source=self.source,
                        _enums=values[f._name],
                    )
                    for f in fields
                    if f._name != self.source.event_name_field
                },
                _source=self.source,
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

    def _get_segment_where_clause(self, table: SA.Table, segment: M.Segment) -> Any:
        if isinstance(segment, M.SimpleSegment):
            s = cast(M.SimpleSegment, segment)
            left = s._left
            evt_name_col = table.columns.get(left._source.event_name_field)
            event_name_filter = (
                (evt_name_col == left._event_name)
                if left._event_name != M.ANY_EVENT_NAME
                else SA.literal(True)
            )
            if s._operator is None:
                return event_name_filter
            else:
                return (event_name_filter) & self._get_simple_segment_condition(
                    table, s
                )
        elif isinstance(segment, M.ComplexSegment):
            c = cast(M.ComplexSegment, segment)
            if c._operator == M.BinaryOperator.AND:
                return self._get_segment_where_clause(
                    table, c._left
                ) & self._get_segment_where_clause(table, c._right)
            else:
                return self._get_segment_where_clause(
                    table, c._left
                ) | self._get_segment_where_clause(table, c._right)
        else:
            raise ValueError(f"Segment of type {type(segment)} is not supported.")

    def _get_timewindow_where_clause(self, table: SA.Table, metric: M.Metric) -> Any:
        start_date = metric._start_dt
        end_date = metric._end_dt

        evt_time_col = table.columns.get(self.source.event_time_field)
        return (evt_time_col >= start_date) & (evt_time_col <= end_date)

    def _get_segmentation_select(self, metric: M.SegmentationMetric) -> Any:
        table = aliased(self.get_table())
        columns = table.columns
        source = self.source

        evt_time_group = (
            self._get_date_trunc(
                table_column=columns.get(source.event_time_field),
                time_group=metric._time_group,
            )
            if metric._time_group != M.TimeGroup.TOTAL
            else SA.literal(None)
        )

        group_by = (
            columns.get(metric._group_by._field._name)
            if metric._group_by is not None
            else SA.literal(None)
        )

        return SA.select(
            columns=[
                evt_time_group.label(GA.DATETIME_COL),
                group_by.label(GA.GROUP_COL),
                SA.func.count(columns.get(source.user_id_field).distinct()).label(
                    GA.USER_COUNT_COL
                ),
                SA.func.count(columns.get(source.user_id_field)).label(
                    GA.EVENT_COUNT_COL
                ),
            ],
            whereclause=(
                self._get_segment_where_clause(table, metric._segment)
                & self._get_timewindow_where_clause(table, metric)
            ),
            group_by=(
                [SA.literal(1), SA.literal(2)]
                if self._column_index_support()
                else [SA.text(GA.DATETIME_COL), SA.text(GA.GROUP_COL)]
            ),
        )

    def _get_conversion_select(self, metric: M.ConversionMetric) -> Any:
        table = self.get_table()
        columns = table.columns
        source = self.source
        first_segment = metric._conversion._segments[0]
        other_segments = metric._conversion._segments[1:]
        user_id_col = columns.get(source.user_id_field)
        event_time_col = columns.get(source.event_time_field)
        time_group = metric._time_group

        if time_group != M.TimeGroup.TOTAL:
            evt_time_group = self._get_date_trunc(
                table_column=event_time_col,
                time_group=time_group,
            )
        else:
            evt_time_group = SA.literal(None)

        group_by = (
            columns.get(metric._group_by._field._name)
            if metric._group_by is not None
            else SA.literal(None)
        )

        steps = [table]
        other_selects = []
        joined_source = table
        for i, seg in enumerate(other_segments):
            prev_table = steps[i]
            prev_cols = prev_table.columns
            curr_table = self.get_table()
            curr_cols = curr_table.columns
            curr_used_id_col = curr_cols.get(source.user_id_field)

            steps.append(curr_table)

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
                curr_table,
                (
                    (prev_cols.get(source.user_id_field) == curr_used_id_col)
                    & (
                        curr_cols.get(source.event_time_field)
                        > prev_cols.get(source.event_time_field)
                    )
                    & (
                        curr_cols.get(source.event_time_field)
                        <= self._get_datetime_interval(
                            columns.get(source.event_time_field), metric._conv_window
                        )
                    )
                    & self._get_segment_where_clause(curr_table, seg)
                ),
                isouter=True,
            )

        columns = [
            evt_time_group.label(GA.DATETIME_COL),
            group_by.label(GA.GROUP_COL),
            (
                SA.func.count(
                    steps[len(steps) - 1].columns.get(source.user_id_field).distinct()
                )
                * 1.0
                / SA.func.count(user_id_col.distinct())
            ).label(GA.CVR_COL),
            SA.func.count(user_id_col.distinct()).label(
                self._fix_col_index(1, GA.USER_COUNT_COL)
            ),
            SA.func.count(user_id_col).label(
                self._fix_col_index(1, GA.EVENT_COUNT_COL)
            ),
        ]

        columns.extend(other_selects)
        return SA.select(
            columns=columns,
            whereclause=(
                self._get_segment_where_clause(table, first_segment)
                & self._get_timewindow_where_clause(table, metric)
            ),
            group_by=(
                [SA.literal(1), SA.literal(2)]
                if self._column_index_support()
                else [SA.text(GA.DATETIME_COL), SA.text(GA.GROUP_COL)]
            ),
        ).select_from(joined_source)
