from __future__ import annotations

import os
import pathlib
import pickle
from abc import ABC
from copy import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar, Union, cast

import pandas as pd
from dateutil.relativedelta import relativedelta

import mitzu.adapters.adapter_factory as factory
import mitzu.adapters.generic_adapter as GA
import mitzu.datasource_discovery as D
import mitzu.helper as helper
import mitzu.notebook.model_loader as ML
import mitzu.visualization as VIS


def default_field(obj, repr: bool = True):
    return field(repr=repr, default_factory=lambda: copy(obj))


ANY_EVENT_NAME = "any_event"


class MetricType(Enum):
    SEGMENTATION = auto()
    CONVERSION = auto()
    RETENTION = auto()
    JOURNEY = auto()


class TimeGroup(Enum):
    TOTAL = auto()
    SECOND = auto()
    MINUTE = auto()
    HOUR = auto()
    DAY = auto()
    WEEK = auto()
    MONTH = auto()
    QUARTER = auto()
    YEAR = auto()

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

    @staticmethod
    def group_by_string(tg: TimeGroup) -> str:
        if tg == TimeGroup.TOTAL:
            return "Overall"
        if tg == TimeGroup.SECOND:
            return "Every Second"
        if tg == TimeGroup.MINUTE:
            return "Every Minute"
        if tg == TimeGroup.HOUR:
            return "Hourly"
        if tg == TimeGroup.DAY:
            return "Daily"
        if tg == TimeGroup.WEEK:
            return "Weekly"
        if tg == TimeGroup.MONTH:
            return "Monthly"
        if tg == TimeGroup.QUARTER:
            return "Quarterly"
        if tg == TimeGroup.YEAR:
            return "Yearly"
        raise Exception("Unkonwn timegroup value exception")


class Operator(Enum):
    EQ = auto()
    NEQ = auto()
    GT = auto()
    LT = auto()
    GT_EQ = auto()
    LT_EQ = auto()
    ANY_OF = auto()
    NONE_OF = auto()
    LIKE = auto()
    NOT_LIKE = auto()
    IS_NULL = auto()
    IS_NOT_NULL = auto()

    def __str__(self) -> str:
        if self == Operator.EQ:
            return "="
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
    AND = auto()
    OR = auto()

    def __str__(self) -> str:
        return self.name.lower()


class DataType(Enum):
    STRING = auto()
    NUMBER = auto()
    BOOL = auto()
    DATETIME = auto()
    MAP = auto()
    STRUCT = auto()

    def is_complex(self) -> bool:
        return self in (DataType.MAP, DataType.STRUCT)


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
    DATABRICKS = "databricks"


@dataclass(frozen=True)
class TimeWindow:
    value: int = 1
    period: TimeGroup = TimeGroup.DAY

    @classmethod
    def parse(cls, val: str | TimeWindow) -> TimeWindow:
        if type(val) == str:
            vals = val.strip().split(" ")
            return TimeWindow(value=int(vals[0]), period=TimeGroup[vals[1].upper()])
        elif type(val) == TimeWindow:
            return val
        else:
            raise ValueError(f"Invalid argument type for TimeWindow parse: {type(val)}")

    def __str__(self) -> str:
        prular = "s" if self.value > 1 else ""
        return f"{self.value} {self.period}{prular}"

    def to_relative_delta(self) -> relativedelta:
        if self.period == TimeGroup.SECOND:
            return relativedelta(seconds=self.value)
        if self.period == TimeGroup.MINUTE:
            return relativedelta(minutes=self.value)
        if self.period == TimeGroup.HOUR:
            return relativedelta(hours=self.value)
        if self.period == TimeGroup.DAY:
            return relativedelta(days=self.value)
        if self.period == TimeGroup.WEEK:
            return relativedelta(weeks=self.value)
        if self.period == TimeGroup.MONTH:
            return relativedelta(months=self.value)
        if self.period == TimeGroup.QUARTER:
            return relativedelta(months=self.value * 4)
        if self.period == TimeGroup.YEAR:
            return relativedelta(year=self.value)
        raise Exception(f"Unsupported relative delta value: {self.period}")


T = TypeVar("T")


@dataclass
class ProtectedState(Generic[T]):
    _value: Optional[T] = None

    def get_value(self) -> Optional[T]:
        return self._value

    def set_value(self, value: Optional[T]):
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
class EventDataTable:
    table_name: str
    event_time_field: Field
    user_id_field: Field
    event_name_field: Optional[Field] = None
    event_name_alias: Optional[str] = None
    ignored_fields: List[str] = default_field([])
    event_specific_fields: List[str] = default_field([])
    description: Optional[str] = None

    @classmethod
    def create(
        cls,
        table_name: str,
        event_time_field: str,
        user_id_field: str,
        event_name_field: str = None,
        event_name_alias: str = None,
        ignored_fields: List[str] = None,
        event_specific_fields: List[str] = None,
        description: str = None,
    ):
        return EventDataTable(
            table_name=table_name,
            event_name_alias=event_name_alias,
            description=description,
            ignored_fields=([] if ignored_fields is None else ignored_fields),
            event_specific_fields=(
                [] if event_specific_fields is None else event_specific_fields
            ),
            event_name_field=Field(_name=event_name_field, _type=DataType.STRING)
            if event_name_field is not None
            else None,
            event_time_field=Field(_name=event_time_field, _type=DataType.DATETIME),
            user_id_field=Field(_name=user_id_field, _type=DataType.STRING),
        )

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

    default_start_dt: Optional[datetime] = None
    default_end_dt: Optional[datetime] = None
    default_property_sample_size: int = 10000
    default_lookback_window: TimeWindow = TimeWindow(30, TimeGroup.DAY)
    default_discovery_lookback_days: int = 2

    _adapter_cache: ProtectedState[GA.GenericDatasetAdapter] = default_field(
        ProtectedState()
    )
    _last_event_times: ProtectedState[Dict[str, datetime]] = default_field(
        ProtectedState()
    )

    _discovered_event_datasource: ProtectedState[
        DiscoveredEventDataSource
    ] = default_field(ProtectedState())

    @property
    def adapter(self) -> GA.GenericDatasetAdapter:
        if not self._adapter_cache.has_value():
            self._adapter_cache.set_value(factory.create_adapter(self))
        res = self._adapter_cache.get_value()
        if res is None:
            raise Exception("Adapter wasn't set for the datasource")
        return res

    def get_default_end_dt(self) -> datetime:
        if self.default_end_dt is None:
            return datetime.now()
        return self.default_end_dt

    def get_default_start_dt(self) -> datetime:
        if self.default_start_dt is None:
            return (
                self.get_default_end_dt()
                - self.default_lookback_window.to_relative_delta()
            )
        return self.default_start_dt

    def get_default_discovery_start_dt(self) -> datetime:
        if self.default_start_dt is None:
            return self.get_default_end_dt() - timedelta(
                self.default_discovery_lookback_days
            )
        return self.default_start_dt

    def clear_adapter_cache(self):
        self._adapter_cache.set_value(None)

    def discover_datasource(self) -> DiscoveredEventDataSource:
        return D.EventDatasourceDiscovery(source=self).discover_datasource()

    def validate(self):
        if len(self.event_data_tables) == 0:
            raise Exception(
                "At least a single EventDataTable needs to be added to the EventDataSource.\n"
                "EventDataSource(event_data_tables = [ EventDataTable.create(...)])"
            )
        for edt in self.event_data_tables:
            edt.validate()


class DatasetModel:
    @classmethod
    def _to_globals(cls, glbs: Dict):
        for k, v in cls.__dict__.items():
            if k != "_to_globals":
                glbs[k] = v


@dataclass(frozen=True, init=False)
class DiscoveredEventDataSource:
    definitions: Dict[EventDataTable, Dict[str, EventDef]]
    source: EventDataSource

    def __init__(
        self,
        definitions: Dict[EventDataTable, Dict[str, EventDef]],
        source: EventDataSource,
    ) -> None:
        object.__setattr__(self, "definitions", definitions)
        object.__setattr__(self, "source", source)
        source._discovered_event_datasource.set_value(self)

    def __post_init__(self):
        self.source.validate()

    def create_notebook_class_model(self) -> DatasetModel:
        return ML.ModelLoader().create_datasource_class_model(self)

    def get_all_events(self) -> Dict[str, EventDef]:
        res: Dict[str, EventDef] = {}
        for val in self.definitions.values():
            res = {**res, **val}
        return res

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
            return cls.load_from_project_binary(file.read())

    @classmethod
    def load_from_project_binary(
        cls, project_binary: bytes
    ) -> DiscoveredEventDataSource:
        res: DiscoveredEventDataSource = pickle.loads(project_binary)
        res.source._discovered_event_datasource.set_value(res)
        return res


@dataclass(frozen=True, init=False)
class Field:
    _name: str
    _type: DataType = field(repr=False)
    _sub_fields: Optional[Tuple[Field, ...]] = None
    _parent: Optional[Field] = field(
        repr=False,
        hash=False,
        default=None,
        compare=False,
    )

    def __init__(
        self,
        _name: str,
        _type: DataType,
        _sub_fields: Optional[Tuple[Field, ...]] = None,
    ):
        object.__setattr__(self, "_name", _name)
        object.__setattr__(self, "_type", _type)
        object.__setattr__(self, "_sub_fields", _sub_fields)
        if _sub_fields is not None:
            for sf in _sub_fields:
                object.__setattr__(sf, "_parent", self)

    def has_sub_field(self, field: Field) -> bool:
        if self._sub_fields is None:
            return False
        curr = field
        while curr._parent is not None:
            curr = curr._parent

        return curr == self

    def __str__(self) -> str:
        if self._sub_fields is not None:
            return "(" + (", ".join([str(f) for f in self._sub_fields])) + ")"
        return f"{self._name} {self._type.name}"

    def _get_name(self) -> str:
        if self._parent is None:
            return self._name
        return f"{self._parent._get_name()}.{self._name}"


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


# =========================================== Metric definitions ===========================================

DEF_MAX_GROUP_COUNT = 10
DEF_LOOK_BACK_DAYS = TimeWindow(30, TimeGroup.DAY)
DEF_CONV_WINDOW = TimeWindow(1, TimeGroup.DAY)
DEF_RET_WINDOW = TimeWindow(1, TimeGroup.WEEK)
DEF_TIME_GROUP = TimeGroup.DAY


@dataclass(frozen=True)
class MetricConfig:
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    lookback_days: Optional[Union[int, TimeWindow]] = None
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
    def _lookback_days(self) -> TimeWindow:
        if self._config.lookback_days is None:
            return DEF_LOOK_BACK_DAYS
        if type(self._config.lookback_days) == int:
            return TimeWindow(self._config.lookback_days, TimeGroup.DAY)
        return cast(TimeWindow, self._config.lookback_days)

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
        if self._config.start_dt is not None:
            return self._config.start_dt
        eds = self._get_event_datasource()
        if eds.default_start_dt is not None:
            return eds.default_start_dt
        return self._end_dt - self._lookback_days.to_relative_delta()

    @property
    def _end_dt(self) -> datetime:
        if self._config.end_dt is not None:
            return self._config.end_dt
        eds = self._get_event_datasource()
        if eds.default_end_dt is not None:
            return eds.default_end_dt
        return datetime.now()

    def _get_event_datasource(self) -> EventDataSource:
        raise NotImplementedError()

    def get_df(self) -> pd.DataFrame:
        raise NotImplementedError()

    def get_sql(self) -> pd.DataFrame:
        raise NotImplementedError()

    def print_sql(self):
        print(self.get_sql())

    def get_figure(self):
        raise NotImplementedError()

    def __repr__(self) -> str:
        fig = self.get_figure()
        fig.show(config={"displayModeBar": False})
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

    def __repr__(self) -> str:
        return super().__repr__()

    def _get_event_datasource(self) -> EventDataSource:
        curr: Segment = self._conversion._segments[0]
        while not isinstance(curr, SimpleSegment):
            curr = cast(ComplexSegment, curr)._left
        return curr._left._source


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

    def __repr__(self) -> str:
        return super().__repr__()

    def _get_event_datasource(self) -> EventDataSource:
        curr: Segment = self._segment
        while not isinstance(curr, SimpleSegment):
            curr = cast(ComplexSegment, curr)._left
        return curr._left._source


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
        lookback_days: Optional[Union[int, TimeWindow]] = None,
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
                ret_window=TimeWindow.parse(ret_window),
            )

            return ret_res
        elif conv_window is not None:
            conv_res = ConversionMetric(conversion=self._conversion, config=config)
            conv_res._conv_window = TimeWindow.parse(conv_window)
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
        lookback_days: Optional[Union[int, TimeWindow]] = None,
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
