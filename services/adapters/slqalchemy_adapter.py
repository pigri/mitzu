from __future__ import annotations
from typing import Any, Dict, List, cast

from services.adapters.generic_adapter import GenericDatasetAdapter
import services.common.model as M
import pandas as pd  # type: ignore
import sqlalchemy as SA  # type: ignore
from sql_formatter.core import format_sql  # type: ignore


def get_enums_agg_dict(
    fields: List[M.Field],
    source: M.EventDataSource,
) -> Dict[str, Any]:
    return {
        f._name: lambda val: set(val)
        if len(set(val)) < source.max_enum_cardinality
        else set()
        for f in fields
    }


class SQLAlchemyAdapter(GenericDatasetAdapter):
    _table: SA.Table
    _engine: Any

    def __init__(self, source: M.EventDataSource):
        super().__init__(source)
        self._table = None
        self._engine = None

    def map_type(self, sa_type: Any) -> M.DataType:
        if isinstance(sa_type, SA.Integer):
            return M.DataType.NUMBER
        if isinstance(sa_type, SA.Float):
            return M.DataType.NUMBER
        if isinstance(sa_type, SA.Text):
            return M.DataType.STRING
        if isinstance(sa_type, SA.DateTime):
            return M.DataType.DATETIME
        raise ValueError(f"{sa_type} is not supported.")

    def execute_query(self, query: Any) -> pd.DataFrame:
        engine = self.get_engine()
        result = engine.execute(query).fetchall()
        return pd.DataFrame(result)

    def get_engine(self) -> Any:
        raise ValueError("Generic SQL Alchemy connections are not yet supported")

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
        return self._table

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
        raise NotImplementedError("Map fields are not supported for file types")

    def get_distinct_event_names(self) -> List[str]:
        table = self.get_table()
        result = self.execute_query(
            SA.select([SA.distinct(table.columns.get(self.source.event_name_field))])
        )
        return pd.DataFrame(result)[self.source.event_name_field].tolist()

    def _get_colum_values_df(
        self, fields: List[M.Field], event_specific: bool
    ) -> pd.DataFrame:
        raise ValueError("Generic SQL Alchemy connections are not yet supported")

    def get_field_enums(
        self, fields: List[M.Field], event_specific: bool
    ) -> Dict[str, M.EventDef]:
        enums = self._get_colum_values_df(fields, event_specific)
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

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        raise NotImplementedError()

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
            return field == None
        if op == M.Operator.IS_NOT_NULL:
            return field != None
        raise ValueError(f"Operator {op} is not supported by SQLAlchemy Adapter.")

    def _get_segment_where_clause(self, table: SA.Table, segment: M.Segment) -> Any:
        if isinstance(segment, M.SimpleSegment):
            s = cast(M.SimpleSegment, segment)
            if s._operator is None:
                return (
                    table.columns.get(s._left._source.event_name_field)
                    == s._left._event_name
                )
            else:
                return (
                    table.columns.get(s._left._source.event_name_field)
                    == s._left._event_name
                ) & self._get_simple_segment_condition(table, s)
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

    def _get_segmentation_select(self, metric: M.SegmentationMetric) -> Any:
        table = self.get_table()
        source = self.source

        evt_time_group = (
            self._get_date_trunc(
                metric._time_group, table.columns.get(source.event_time_field)
            )
            if metric._time_group != M.TimeGroup.TOTAL
            else SA.literal(None)
        )

        group_by = (
            table.columns.get(metric._group_by._field._name)
            if metric._group_by is not None
            else SA.literal(None)
        )

        return SA.select(
            columns=[
                evt_time_group.label("datetime"),
                group_by.label("group"),
                SA.func.count(table.columns.get(source.user_id_field).distinct()).label(
                    "unique_user_count"
                ),
                SA.func.count(table.columns.get(source.user_id_field)).label(
                    "event_count"
                ),
            ],
            whereclause=self._get_segment_where_clause(table, metric._segment),
            group_by=[evt_time_group, group_by],
        )

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
