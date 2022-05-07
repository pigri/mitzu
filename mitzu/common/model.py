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
import mitzu.notebook.visualization as VIS
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
    def parse(cls, val: Optional[str | TimeGroup]) -> Optional[TimeGroup]:
        if val is None:
            return None
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
    _last_event_times: ProtectedState[Dict[str, datetime]] = default_field(
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

    def get_last_event_times(self) -> Dict[str, datetime]:
        if self._last_event_times.get_value() is None:
            lets = self.adapter.get_last_event_times()
            self._last_event_times.set_value(lets)

        res = self._last_event_times.get_value()
        return res if res is not None else {}

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


class DatasetModel(ABC):
    @classmethod
    def _to_globals(cls, glbs: Dict):
        for k, v in cls.__dict__.items():
            if k != "_to_globals":
                glbs[k] = v


@dataclass(frozen=True)
class DiscoveredEventDataSource:
    definitions: Dict[EventDataTable, Dict[str, EventDef]]
    source: EventDataSource

    def __post_init__(self):
        self.source.validate()

    def create_notebook_class_model(self) -> DatasetModel:
        return ML.ModelLoader().create_datasource_class_model(self)

    @staticmethod
    def _get_path(project: str, folder: str = "./", extension="mitzu") -> pathlib.Path:
        return pathlib.Path(folder, f"{project}.{extension}")

    def save_project(self, project: str, folder: str = "./", extension="mitzu"):
        path = self._get_path(project, folder, extension)
        with path.open(mode="wb") as file:
            pickle.dump(self, file)

    @classmethod
    def load_from_project_file(
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
DEF_LOOK_BACK_DAYS = 30
DEF_CONV_WINDOW = TimeWindow(1, TimeGroup.DAY)
DEF_RET_WINDOW = TimeWindow(1, TimeGroup.WEEK)
DEF_TIME_GROUP = TimeGroup.DAY


@dataclass(frozen=True)
class MetricConfig:
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    lookback_days: Optional[int] = None
    time_group: Optional[TimeGroup] = None
    max_group_count: Optional[int] = None
    group_by: Optional[EventFieldDef] = None
    custom_title: Optional[str] = None


@dataclass(init=False, frozen=True)
class Metric(ABC):
    _config: MetricConfig

    def __init__(self, config: MetricConfig):
        object.__setattr__(self, "_config", config)

    @property
    def _max_group_count(self) -> int:
        if self._config.max_group_count is None:
            return DEF_MAX_GROUP_COUNT
        return self._config.max_group_count

    @property
    def _lookback_days(self) -> int:
        if self._config.lookback_days is None:
            return DEF_LOOK_BACK_DAYS
        return self._config.lookback_days

    @property
    def _time_group(self) -> TimeGroup:
        if self._config.time_group is None:
            # TBD TG calc
            return DEF_TIME_GROUP
        return self._config.time_group

    @property
    def _group_by(self) -> Optional[EventFieldDef]:
        return self._config.group_by

    @property
    def _custom_title(self) -> Optional[str]:
        return self._config.custom_title

    @property
    def _start_dt(self) -> datetime:
        return (
            self._config.start_dt
            if self._config.start_dt is not None
            else self._end_dt - timedelta(days=self._lookback_days)
        )

    @property
    def _end_dt(self) -> datetime:
        raise NotImplementedError()

    def get_df(self) -> pd.DataFrame:
        raise NotImplementedError()

    def get_sql(self) -> pd.DataFrame:
        raise NotImplementedError()

    def print_sql(self):
        print(self.get_sql(self))

    def get_figure(self):
        raise NotImplementedError()

    def __repr__(self) -> str:
        fig = self.get_figure()
        fig.show()
        return ""


class ConversionMetric(Metric):
    _conversion: Conversion
    _conv_window: TimeWindow

    def __init__(
        self,
        conversion: Conversion,
        config: MetricConfig,
        conv_window: TimeWindow = DEF_CONV_WINDOW,
    ):
        super().__init__(config)
        self._conversion = conversion
        self._conv_window = conv_window

    def get_df(self) -> pd.DataFrame:
        source = helper.get_segment_event_datasource(self._conversion._segments[0])
        return source.adapter.get_conversion_df(self)

    def get_sql(self) -> pd.DataFrame:
        source = helper.get_segment_event_datasource(self._conversion._segments[0])
        return source.adapter.get_conversion_sql(self)

    def get_figure(self):
        return VIS.plot_conversion(self)

    @property
    def _end_dt(self) -> datetime:
        if self._config.end_dt is not None:
            return self._config.end_dt
        return helper.get_segment_end_date(self._conversion._segments[0])

    def __repr__(self) -> str:
        return super().__repr__()


@dataclass(frozen=True, init=False)
class SegmentationMetric(Metric):
    _segment: Segment

    def __init__(self, segment: Segment, config: MetricConfig):
        super().__init__(config)
        object.__setattr__(self, "_segment", segment)

    def get_df(self) -> pd.DataFrame:
        source = helper.get_segment_event_datasource(self._segment)
        return source.adapter.get_segmentation_df(self)

    def get_sql(self) -> str:
        source = helper.get_segment_event_datasource(self._segment)
        return source.adapter.get_segmentation_sql(self)

    def get_figure(self):
        return VIS.plot_segmentation(self)

    @property
    def _end_dt(self) -> datetime:
        if self._config.end_dt is not None:
            return self._config.end_dt
        return helper.get_segment_end_date(self._segment)

    def __repr__(self) -> str:
        return super().__repr__()


@dataclass(frozen=True, init=False)
class RetentionMetric(Metric):

    _conversion: Conversion
    _ret_window: TimeWindow

    def __init__(
        self,
        conversion: Conversion,
        config: MetricConfig,
        ret_window: TimeWindow = DEF_RET_WINDOW,
    ):
        super().__init__(config)
        object.__setattr__(self, "_conversion", conversion)
        object.__setattr__(self, "_ret_window", ret_window)

    @property
    def _end_dt(self) -> datetime:
        if self._config.end_dt is not None:
            return self._config.end_dt
        return helper.get_segment_end_date(self._conversion._segments[0])

    def __repr__(self) -> str:
        return super().__repr__()


class Conversion(ConversionMetric):
    def __init__(self, segments: List[Segment]):
        super().__init__(self, config=MetricConfig())
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
        time_group: Optional[str | TimeGroup] = None,
        group_by: Optional[EventFieldDef] = None,
        max_group_by_count: Optional[int] = None,
        lookback_days: Optional[int] = None,
        custom_title: Optional[str] = None,
    ) -> ConversionMetric | RetentionMetric:
        config = MetricConfig(
            start_dt=helper.parse_datetime_input(start_dt, None),
            end_dt=helper.parse_datetime_input(end_dt, None),
            time_group=TimeGroup.parse(time_group),
            group_by=group_by,
            max_group_count=max_group_by_count,
            custom_title=custom_title,
            lookback_days=lookback_days,
        )
        if ret_window is not None:
            ret_res = RetentionMetric(
                conversion=self._conversion,
                config=config,
                ret_window=TimeWindow.parse_input(ret_window),
            )

            return ret_res
        elif conv_window is not None:
            conv_res = ConversionMetric(conversion=self._conversion, config=config)
            conv_res._conv_window = TimeWindow.parse_input(conv_window)
            return conv_res
        else:
            raise ValueError("conw_window or ret_window must be defined")

    def __repr__(self) -> str:
        return super().__repr__()


class Segment(SegmentationMetric):
    def __init__(self):
        super().__init__(self, config=MetricConfig())

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
        time_group: Optional[str | TimeGroup] = None,
        group_by: Optional[EventFieldDef] = None,
        max_group_by_count: Optional[int] = None,
        lookback_days: Optional[int] = None,
        custom_title: Optional[str] = None,
    ) -> SegmentationMetric:
        config = MetricConfig(
            start_dt=helper.parse_datetime_input(start_dt, None),
            end_dt=helper.parse_datetime_input(end_dt, None),
            time_group=TimeGroup.parse(time_group),
            group_by=group_by,
            max_group_count=max_group_by_count,
            custom_title=custom_title,
            lookback_days=lookback_days,
        )

        return SegmentationMetric(segment=self, config=config)

    def __repr__(self) -> str:
        return super().__repr__()


@dataclass(init=False, frozen=True)
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


@dataclass(init=False, frozen=True)
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
