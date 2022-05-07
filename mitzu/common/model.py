from __future__ import annotations

import os
import pathlib
import pickle
from abc import ABC
from copy import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar

import mitzu.adapters.adapter_factory as factory
import mitzu.adapters.generic_adapter as GA
import mitzu.common.helper as helper
import mitzu.discovery.datasource_discovery as D
import mitzu.notebook.model_loader as ML
import mitzu.notebook.visualization as vis
import pandas as pd


def default_field(obj, repr: bool = True):
    return field(repr=repr, default_factory=lambda: copy(obj))


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
    FILE = "file"
    ATHENA = "awsathena+rest"
    TRINO = "trino"
    POSTGRESQL = "postgresql+psycopg2"
    MYSQL = "mysql+mysqlconnector"
    SQLITE = "sqlite"


T = TypeVar("T")


@dataclass
class ProtectedState(Generic[T]):
    _value: Optional[T] = None

    def get_value(self) -> Optional[T]:
        return self._value

    def set_value(self, value: T):
        self._value = value

    def has_value(self) -> bool:
        return self._value is not None

    def __getstate__(self):
        """Override so pickle doesn't store state"""
        return None

    def __str__(self) -> str:
        return ""

    def __repr__(self) -> str:
        return ""


class SecretResolver(ABC):
    def resolve_secret(self) -> str:
        raise NotImplementedError()


@dataclass(frozen=True)
class PromptSecretResolver(SecretResolver):
    title: str

    def resolve_secret(self) -> str:
        import getpass

        return getpass.getpass(prompt=self.title)


@dataclass(frozen=True)
class ConstSecretResolver(SecretResolver):
    secret: str

    def resolve_secret(self) -> str:
        return self.secret


@dataclass(frozen=True)
class EnvVarSecretResolver(SecretResolver):
    variable_name: str

    def resolve_secret(self) -> str:
        secret = os.getenv(self.variable_name)
        if secret is not None:
            return secret
        else:
            raise Exception(f"Environmental variable {self.variable_name} was not set.")


@dataclass(frozen=True)
class Connection:

    connection_type: ConnectionType
    user_name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    url: Optional[str] = None
    schema: Optional[str] = None
    # Used for connection url parametrization
    url_params: Optional[str] = None
    # Used for adapter configuration
    extra_configs: Dict[str, Any] = default_field({})
    _secret: ProtectedState[str] = default_field(ProtectedState())
    secret_resolver: Optional[SecretResolver] = None

    @property
    def password(self):
        if not self._secret.has_value():
            if self.secret_resolver is not None:
                secret = self.secret_resolver.resolve_secret()
                self._secret.set_value(secret)
            else:
                return ""
        return self._secret.get_value()


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class EventDataTable:
    table_name: str
    event_time_field: str
    user_id_field: str
    event_name_field: Optional[str] = None
    event_name_alias: Optional[str] = None
    ignored_fields: List[str] = default_field([])
    event_specific_fields: List[str] = default_field([])
    description: Optional[str] = None

    def __hash__(self):
        return hash(
            f"{self.table_name}{self.event_time_field}{self.user_id_field}"
            f"{self.event_name_field}{self.event_name_alias}"
        )

    def validate(self):
        if self.event_name_alias is not None and self.event_name_field is not None:
            raise Exception(
                f"For {self.table_name} both event_name_alias and event_name_field can't be defined in the same time."
            )
        if self.event_name_alias is None and self.event_name_field is None:
            raise Exception(
                f"For {self.table_name} define the event_name_alias or the event_name_field property."
            )


@dataclass(frozen=True)
class EventDataSource:
    connection: Connection
    event_data_tables: List[EventDataTable]
    max_enum_cardinality: int = 300
    max_map_key_cardinality: int = 300
    _adapter_cache: ProtectedState[GA.GenericDatasetAdapter] = default_field(
        ProtectedState()
    )

    @property
    def adapter(self) -> GA.GenericDatasetAdapter:
        if not self._adapter_cache.has_value():
            self._adapter_cache.set_value(factory.create_adapter(self))
        res = self._adapter_cache.get_value()
        if res is None:
            raise Exception("Adapter wasn't set for the datasource")
        return res

    def clear_adapter_cache(self):
        self._adapter_cache.set_value(None)

    def discover_datasource(
        self, start_dt: datetime = None, end_dt: datetime = None
    ) -> DiscoveredEventDataSource:
        return D.EventDatasourceDiscovery(
            source=self, start_dt=start_dt, end_dt=end_dt
        ).discover_datasource()

    def validate(self):
        if len(self.event_data_tables) == 0:
            raise Exception(
                "At least a single EventDataTable needs to be added to the EventDataSource.\n"
                "EventDataSource(event_data_tables = [ EventDataTable(...)])"
            )
        for edt in self.event_data_tables:
            edt.validate()


@dataclass(frozen=True)
class DiscoveredEventDataSource:
    definitions: Dict[EventDataTable, Dict[str, EventDef]]
    source: EventDataSource

    def __post_init__(self):
        self.source.validate()

    def create_notebook_class_model(self) -> ML.DatasetModel:
        return ML.ModelLoader().create_datasource_class_model(self)

    @staticmethod
    def _get_path(project: str, folder: str = "./", extension="mitzu") -> pathlib.Path:
        return pathlib.Path(folder, f"{project}.{extension}")

    def to_pickle(self, project: str, folder: str = "./", extension="mitzu"):
        path = self._get_path(project, folder, extension)
        with path.open(mode="wb") as file:
            pickle.dump(self, file)

    @classmethod
    def from_pickle(
        cls, project: str, folder: str = "./", extension="mitzu"
    ) -> DiscoveredEventDataSource:
        path = cls._get_path(project, folder, extension)
        with path.open(mode="rb") as file:
            return pickle.load(file)


@dataclass(frozen=True)
class Field:
    _name: str
    _type: DataType = field(repr=False)

    def __hash__(self) -> int:
        return hash(f"{self._name}{self._type}")


@dataclass(frozen=True)
class EventFieldDef:
    _event_name: str
    _field: Field
    _source: EventDataSource
    _event_data_table: EventDataTable
    _description: Optional[str] = ""
    _enums: Optional[List[Any]] = None


@dataclass(frozen=True)
class EventDef:
    _event_name: str
    _fields: Dict[Field, EventFieldDef]
    _source: EventDataSource
    _event_data_table: EventDataTable
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
        return source.adapter.get_conversion_df(self)

    def get_sql(self) -> pd.DataFrame:
        source = helper.get_segment_event_datasource(self._conversion._segments[0])
        return source.adapter.get_conversion_sql(self)

    def print_sql(self):
        source = helper.get_segment_event_datasource(self._conversion._segments[0])
        print(source.adapter.get_conversion_sql(self))

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
        return source.adapter.get_segmentation_df(self)

    def get_sql(self) -> str:
        source = helper.get_segment_event_datasource(self._segment)
        return source.adapter.get_segmentation_sql(self)

    def print_sql(self):
        source = helper.get_segment_event_datasource(self._segment)
        print(source.adapter.get_segmentation_sql(self))

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


@dataclass(init=False)
class ComplexSegment(Segment):
    _left: Segment
    _operator: BinaryOperator
    _right: Segment

    def __init__(self, _left: Segment, _operator: BinaryOperator, _right: Segment):
        object.__setattr__(self, "_left", _left)
        object.__setattr__(self, "_operator", _operator)
        object.__setattr__(self, "_right", _right)
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()


@dataclass(init=False)
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
        object.__setattr__(self, "_left", _left)
        object.__setattr__(self, "_operator", _operator)
        object.__setattr__(self, "_right", _right)
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()
