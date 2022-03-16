from __future__ import annotations
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from copy import copy

import pandas as pd  # type: ignore
import services.common.helper as helper
import services.adapters.adapter_factory as factory
import services.notebook.visualization as vis


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


class BinaryOperator(Enum):
    AND = 1
    OR = 2


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
    POSTGRES = 6
    BIG_QUERY = 7
    CLICKHOUSE = 8
    PANDAS_DF = 9


@dataclass(unsafe_hash=True)
class Field:
    name: str
    type: DataType
    parent: Optional[Field] = None

    def __str__(self) -> str:
        if self.parent:
            return f"{self.parent}.{self.name}"
        else:
            return self.name

    def __repr__(self) -> str:
        return str(self)


@dataclass
class Connection:

    connection_type: ConnectionType
    url: str
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


@dataclass
class CombinedTimeWindow:
    windows: List[TimeWindow]


@dataclass
class EventDataSource:
    connection: Connection
    table_name: str
    event_time_field: str = "event_time"
    user_id_field: str = "user_id"
    event_name_field: str = "event_name"
    ignored_fields: List[str] = default_field([])
    event_specific_fields: List[str] = default_field([])
    event_id_field: Optional[str] = None
    max_enum_cardinality: int = 300
    max_map_key_cardinality: int = 300
    description: Optional[str] = None


@dataclass
class DiscoveredDataset:

    definitions: Dict[str, EventDef]
    source: EventDataSource

    def __str__(self) -> str:
        return "\n".join([str(v) for v in self.definitions.values()])


@dataclass
class EventFieldDef:
    event_name: str
    field: Field
    source: EventDataSource
    description: Optional[str] = ""
    enums: Optional[List[Any]] = None


@dataclass
class EventDef:
    event_name: str
    fields: Dict[Field, EventFieldDef]
    source: EventDataSource
    description: Optional[str] = ""

    def __str__(self) -> str:
        fields_str = ",".join(
            [f"{fi.name}: {evd.enums}" for fi, evd in self.fields.items()]
        )
        return f"{self.event_name}: {fields_str}"

    def __repr__(self) -> str:
        return str(self)


# =========================================== Metric definitions ===========================================

DEF_MAX_GROUP_COUNT = 10
DEF_LOOK_BACK_DAYS = 30
DEF_CONV_WINDOW = TimeWindow(1, TimeGroup.DAY)
DEF_RET_WINDOW = TimeWindow(1, TimeGroup.WEEK)
DEF_TIME_GROUP = TimeGroup.TOTAL


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
        adapter = factory.create_adapter(source=source)
        return adapter.get_conversion_df(self)

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
        adapter = factory.create_adapter(source=source)
        return adapter.get_segmentation_df(self)

    def get_sql(self) -> str:
        source = helper.get_segment_event_datasource(self._segment)
        adapter = factory.create_adapter(source=source)
        return adapter.get_segmentation_sql(self)

    def print_sql(self):
        source = helper.get_segment_event_datasource(self._segment)
        adapter = factory.create_adapter(source=source)
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

    def conversion(
        self,
        conv_window: str | TimeWindow = DEF_CONV_WINDOW,
        start_dt: Optional[str | datetime] = None,
        end_dt: Optional[str | datetime] = None,
        time_group: str | TimeGroup = DEF_TIME_GROUP,
        group_by: EventFieldDef = None,
        max_group_by_count: int = DEF_MAX_GROUP_COUNT,
        custom_title: str = None,
    ) -> ConversionMetric:
        res = ConversionMetric(conversion=self._conversion)
        res._conv_window = TimeWindow.parse_input(conv_window)
        res._start_dt = helper.parse_datetime_input(
            start_dt, datetime.now() - timedelta(days=DEF_LOOK_BACK_DAYS)
        )
        res._end_dt = helper.parse_datetime_input(end_dt, datetime.now())
        res._time_group = TimeGroup.parse(time_group)
        res._group_by = group_by
        res._max_group_count = max_group_by_count
        res._custom_title = custom_title
        return res

    def retention(
        self,
        ret_window: str | TimeWindow = DEF_RET_WINDOW,
        start_dt: Optional[str | datetime] = None,
        end_dt: Optional[str | datetime] = None,
        time_group: str | TimeGroup = DEF_TIME_GROUP,
        group_by: EventFieldDef = None,
        max_group_by_count: int = DEF_MAX_GROUP_COUNT,
        custom_title: str = None,
    ) -> RetentionMetric:
        res = RetentionMetric(conversion=self._conversion)
        res._ret_window = TimeWindow.parse_input(ret_window)
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


class Segment(SegmentationMetric):
    def __init__(self):
        super().__init__(self)

    def __and__(self, right: Segment) -> ComplexSegment:
        return ComplexSegment(self, BinaryOperator.AND, right)

    def __or__(self, right: Segment) -> ComplexSegment:
        return ComplexSegment(self, BinaryOperator.OR, right)

    def __rshift__(self, right: Segment) -> Conversion:
        return Conversion([self, right])

    def segmentation(
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

    def __repr__(self) -> str:
        return super().__repr__()


@dataclass
class SimpleSegment(Segment):
    _left: EventFieldDef | EventDef  # str is an event_name without any filters
    _operator: Optional[Operator] = None
    _right: Optional[Any] = None

    def __repr__(self) -> str:
        return super().__repr__()
