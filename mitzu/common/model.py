from __future__ import annotations
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from enum import Enum
from copy import copy

import pandas as pd  # type: ignore
import mitzu.common.helper as helper
import mitzu.adapters.adapter_factory as factory
import mitzu.notebook.visualization as vis
import mitzu.adapters.generic_adapter as GA


def default_field(obj):
    return field(default_factory=lambda: copy(obj))


ANY_EVENT_NAME = "any_event"


class MetricType(Enum):
    SEGMENTATION = 1
    CONVERSION = 2
    RETENTION = 3
    JOURNEY = 4


class TimeGroup(Enum):
    TOTAL = 1
    SECOND = 2
    MINUTE = 3
    HOUR = 4
    DAY = 5
    WEEK = 6
    MONTH = 7
    QUARTER = 8
    YEAR = 9

    @classmethod
    def parse(cls, val: str | TimeGroup) -> TimeGroup:
        if type(val) == TimeGroup:
            return val
        elif type(val) == str:
            return TimeGroup[val.upper()]
        else:
            raise ValueError(f"Invalid argument type for TimeGroup parse: {type(val)}")

    def __str__(self) -> str:
        return self.name.lower()


class Operator(Enum):
    EQ = 1
    NEQ = 2
    GT = 3
    LT = 4
    GT_EQ = 4
    LT_EQ = 5
    ANY_OF = 6
    NONE_OF = 7
    LIKE = 8
    NOT_LIKE = 9
    IS_NULL = 10
    IS_NOT_NULL = 11

    def __str__(self) -> str:
        if self == Operator.EQ:
            return "=="
        if self == Operator.NEQ:
            return "!="
        if self == Operator.GT:
            return ">"
        if self == Operator.LT:
            return "<"
        if self == Operator.GT_EQ:
            return ">="
        if self == Operator.LT_EQ:
            return "<="
        if self == Operator.ANY_OF:
            return "any of"
        if self == Operator.NONE_OF:
            return "none of"
        if self == Operator.LIKE:
            return "not like"
        if self == Operator.NOT_LIKE:
            return "not like"
        if self == Operator.IS_NULL:
            return "is null"
        if self == Operator.IS_NOT_NULL:
            return "is not null"

        raise ValueError(f"Not supported operator for title: {self}")


class BinaryOperator(Enum):
    AND = 1
    OR = 2

    def __str__(self) -> str:
        return self.name.lower()


class DataType(Enum):
    STRING = 1
    NUMBER = 2
    BOOL = 3
    DATETIME = 4
    MAP = 5
    STRUCT = 6
    ARRAY = 7


class AttributionMode(Enum):
    FIRST_EVENT = 1
    LAST_EVENT = 2
    ALL_EVENTS = 3


class ConnectionType(Enum):
    FILE = 1
    ATHENA = 2
    DATABRICKS_SQL = 3
    SNOWFLAKE = 4
    REDSHIFT = 5
    POSTGRESQL = 6
    BIG_QUERY = 7
    CLICKHOUSE = 8
    PANDAS_DF = 9


@dataclass
class Connection:

    connection_type: ConnectionType
    connection_params: Dict = default_field({})


@dataclass
class TimeWindow:
    value: int = 1
    period: TimeGroup = TimeGroup.DAY

    @classmethod
    def parse_input(cls, val: str | TimeWindow) -> TimeWindow:
        if type(val) == str:
            vals = val.strip().split(" ")
            return TimeWindow(value=int(vals[0]), period=TimeGroup[vals[1].upper()])
        elif type(val) == TimeWindow:
            return val
        else:
            raise ValueError(f"Invalid argument type for TimeWindow parse: {type(val)}")

    def __str__(self) -> str:
        return f"{self.value} {self.period}"


@dataclass
class CombinedTimeWindow:
    windows: List[TimeWindow]


@dataclass
class EventDataSource:
    connection: Connection
    table_name: str = "dataset"
    event_time_field: str = "event_time"
    user_id_field: str = "user_id"
    event_name_field: str = "event_name"
    ignored_fields: List[str] = default_field([])
    event_specific_fields: List[str] = default_field([])
    event_id_field: Optional[str] = None
    max_enum_cardinality: int = 300
    max_map_key_cardinality: int = 300
    description: Optional[str] = None
    adapter: Optional[GA.GenericDatasetAdapter] = None


@dataclass
class DiscoveredDataset:

    definitions: Dict[str, EventDef]
    source: EventDataSource

    def __str__(self) -> str:
        return "\n".join([str(v) for v in self.definitions.values()])


@dataclass(unsafe_hash=True)
class Field:
    _name: str
    _type: DataType
    _parent: Optional[Field] = None

    def __str__(self) -> str:
        if self._parent:
            return f"{self._parent}.{self._name}"
        else:
            return self._name

    def __repr__(self) -> str:
        return str(self)


@dataclass
class EventFieldDef:
    _event_name: str
    _field: Field
    _source: EventDataSource
    _description: Optional[str] = ""
    _enums: Optional[List[Any]] = None


@dataclass
class EventDef:
    _event_name: str
    _fields: Dict[Field, EventFieldDef]
    _source: EventDataSource
    _description: Optional[str] = ""

    def __str__(self) -> str:
        fields_str = ",".join(
            [f"{fi._name}: {evd._enums}" for fi, evd in self._fields.items()]
        )
        return f"{self._event_name}: {fields_str}"

    def __repr__(self) -> str:
        return str(self)


# =========================================== Metric definitions ===========================================

DEF_MAX_GROUP_COUNT = 10
DEF_LOOK_BACK_DAYS = 365
DEF_CONV_WINDOW = TimeWindow(1, TimeGroup.DAY)
DEF_RET_WINDOW = TimeWindow(1, TimeGroup.WEEK)
DEF_TIME_GROUP = TimeGroup.DAY


class Metric:
    def __init__(self):
        # Default specified values, used if no config is applied
        self._start_dt = datetime.now() - timedelta(days=DEF_LOOK_BACK_DAYS)
        self._end_dt = datetime.now()
        self._time_group: TimeGroup = DEF_TIME_GROUP
        self._max_group_count: int = DEF_MAX_GROUP_COUNT
        self._group_by: Optional[EventFieldDef] = None
        self._custom_title: Optional[str] = None


class ConversionMetric(Metric):
    def __init__(
        self,
        conversion: Conversion,
    ):
        super().__init__()
        self._conversion = conversion
        self._conv_window: TimeWindow = DEF_CONV_WINDOW

    def get_df(self) -> pd.DataFrame:
        source = helper.get_segment_event_datasource(self._conversion._segments[0])
        adapter = factory.get_or_create_adapter(source=source)
        return adapter.get_conversion_df(self)

    def get_sql(self) -> pd.DataFrame:
        source = helper.get_segment_event_datasource(self._conversion._segments[0])
        adapter = factory.get_or_create_adapter(source=source)
        return adapter.get_conversion_sql(self)

    def print_sql(self):
        source = helper.get_segment_event_datasource(self._conversion._segments[0])
        adapter = factory.get_or_create_adapter(source=source)
        print(adapter.get_conversion_sql(self))

    def __repr__(self) -> str:
        fig = vis.plot_conversion(self)
        fig.show()
        return ""


class SegmentationMetric(Metric):
    def __init__(
        self,
        segment: Segment,
    ):
        super().__init__()
        self._segment = segment

    def get_df(self) -> pd.DataFrame:
        source = helper.get_segment_event_datasource(self._segment)
        adapter = factory.get_or_create_adapter(source=source)
        return adapter.get_segmentation_df(self)

    def get_sql(self) -> str:
        source = helper.get_segment_event_datasource(self._segment)
        adapter = factory.get_or_create_adapter(source=source)
        return adapter.get_segmentation_sql(self)

    def print_sql(self):
        source = helper.get_segment_event_datasource(self._segment)
        adapter = factory.get_or_create_adapter(source=source)
        print(adapter.get_segmentation_sql(self))

    def __repr__(self) -> str:
        fig = vis.plot_segmentation(self)
        fig.show()
        return ""


class RetentionMetric(Metric):
    def __init__(
        self,
        conversion: Conversion,
    ):
        super().__init__()
        self._conversion = conversion
        self._ret_window: TimeWindow = DEF_RET_WINDOW


class Conversion(ConversionMetric):
    def __init__(self, segments: List[Segment]):
        super().__init__(self)
        self._segments = segments

    def __rshift__(self, right: Segment) -> Conversion:
        segments = copy(self._segments)
        segments.append(right)
        return Conversion(segments)

    def config(
        self,
        conv_window: Optional[str | TimeWindow] = DEF_CONV_WINDOW,
        ret_window: Optional[str | TimeWindow] = None,
        start_dt: Optional[str | datetime] = None,
        end_dt: Optional[str | datetime] = None,
        time_group: str | TimeGroup = DEF_TIME_GROUP,
        group_by: EventFieldDef = None,
        max_group_by_count: int = DEF_MAX_GROUP_COUNT,
        custom_title: str = None,
    ) -> ConversionMetric | RetentionMetric:
        if ret_window is not None:
            ret_res = RetentionMetric(conversion=self._conversion)
            ret_res._ret_window = TimeWindow.parse_input(ret_window)
            ret_res._start_dt = helper.parse_datetime_input(
                start_dt, datetime.now() - timedelta(days=DEF_LOOK_BACK_DAYS)
            )
            ret_res._end_dt = helper.parse_datetime_input(end_dt, datetime.now())
            ret_res._time_group = TimeGroup.parse(time_group)
            ret_res._group_by = group_by
            ret_res._max_group_count = max_group_by_count
            ret_res._custom_title = custom_title
            return ret_res
        else:
            if conv_window is None:
                raise ValueError(
                    "Conversion window must be defined.\n e.g. \".config(conv_window='1 day')\""
                )
            conv_res = ConversionMetric(conversion=self._conversion)
            conv_res._conv_window = TimeWindow.parse_input(conv_window)
            conv_res._start_dt = helper.parse_datetime_input(
                start_dt, datetime.now() - timedelta(days=DEF_LOOK_BACK_DAYS)
            )
            conv_res._end_dt = helper.parse_datetime_input(end_dt, datetime.now())
            conv_res._time_group = TimeGroup.parse(time_group)
            conv_res._group_by = group_by
            conv_res._max_group_count = max_group_by_count
            conv_res._custom_title = custom_title
            return conv_res

    def __repr__(self) -> str:
        return super().__repr__()


class Segment(SegmentationMetric):
    def __init__(self):
        super().__init__(self)

    def __and__(self, right: Segment) -> ComplexSegment:
        return ComplexSegment(self, BinaryOperator.AND, right)

    def __or__(self, right: Segment) -> ComplexSegment:
        return ComplexSegment(self, BinaryOperator.OR, right)

    def __rshift__(self, right: Segment) -> Conversion:
        return Conversion([self, right])

    def config(
        self,
        start_dt: Optional[str | datetime] = None,
        end_dt: Optional[str | datetime] = None,
        time_group: str | TimeGroup = DEF_TIME_GROUP,
        group_by: EventFieldDef = None,
        max_group_by_count: int = DEF_MAX_GROUP_COUNT,
        custom_title: str = None,
    ) -> SegmentationMetric:
        res = SegmentationMetric(segment=self)
        res._start_dt = helper.parse_datetime_input(
            start_dt, datetime.now() - timedelta(days=DEF_LOOK_BACK_DAYS)
        )
        res._end_dt = helper.parse_datetime_input(end_dt, datetime.now())
        res._time_group = TimeGroup.parse(time_group)
        res._group_by = group_by
        res._max_group_count = max_group_by_count
        res._custom_title = custom_title
        return res

    def __repr__(self) -> str:
        return super().__repr__()


@dataclass
class ComplexSegment(Segment):
    _left: Segment
    _operator: BinaryOperator
    _right: Segment

    def __init__(self, _left: Segment, _operator: BinaryOperator, _right: Segment):
        super().__init__()
        self._left = _left
        self._right = _right
        self._operator = _operator

    def __repr__(self) -> str:
        return super().__repr__()


@dataclass
class SimpleSegment(Segment):
    _left: EventFieldDef | EventDef  # str is an event_name without any filters
    _operator: Optional[Operator] = None
    _right: Optional[Any] = None

    def __init__(
        self,
        _left: EventFieldDef | EventDef,
        _operator: Optional[Operator] = None,
        _right: Optional[Any] = None,
    ):
        super().__init__()
        self._left = _left
        self._right = _right
        self._operator = _operator

    def __repr__(self) -> str:
        return super().__repr__()
