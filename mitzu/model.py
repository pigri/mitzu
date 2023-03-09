from __future__ import annotations

import logging
import os
import pathlib
from urllib import parse
from abc import ABC
from copy import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    Protocol,
)
import pandas as pd

from dateutil import parser
from dateutil.relativedelta import relativedelta

import mitzu.adapters.adapter_factory as factory
import mitzu.adapters.generic_adapter as GA
import mitzu.helper as helper
import mitzu.notebook.model_loader as ML
import mitzu.project_discovery as D
import mitzu.visualization.charts as CHRT
import mitzu.visualization.plot as PLT
import mitzu.visualization.titles as TI
import mitzu.project_serialization as PSE


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
    def parse(cls, val: Union[str, TimeGroup]) -> TimeGroup:
        if type(val) == TimeGroup:
            return val
        elif type(val) == str:
            val = val.upper()
            if val.endswith("S"):
                val = val[:-1]
            return TimeGroup[val]
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


class AggType(Enum):

    COUNT_UNIQUE_USERS = auto()
    COUNT_EVENTS = auto()
    CONVERSION = auto()
    RETENTION_RATE = auto()
    PERCENTILE_TIME_TO_CONV = auto()
    AVERAGE_TIME_TO_CONV = auto()

    def to_agg_str(self, agg_param: Any = None) -> str:
        if self == AggType.CONVERSION:
            return "conversion"
        if self == AggType.RETENTION_RATE:
            return "retention_rate"
        if self == AggType.COUNT_EVENTS:
            return "event_count"
        if self == AggType.COUNT_UNIQUE_USERS:
            return "user_count"
        if self == AggType.AVERAGE_TIME_TO_CONV:
            return "ttc_avg"
        if self == AggType.PERCENTILE_TIME_TO_CONV:
            if agg_param is None:
                raise ValueError(
                    "For percentile time to convert aggregation agg_param is required"
                )
            return f"ttc_p{agg_param:.0f}"
        raise ValueError(f"{self} is not supported for to str")

    @staticmethod
    def parse_agg_str(val: str) -> Tuple[AggType, Any]:
        val = val.lower()
        if val == "event_count":
            return (AggType.COUNT_EVENTS, None)
        if val == "user_count":
            return (AggType.COUNT_UNIQUE_USERS, None)
        if val.startswith("ttc_p") and val[5:].isnumeric():
            param = int(val[5:])
            if 0 < param > 100:
                raise ValueError("Percentile value must be an integer between 0 and 99")
            return (AggType.PERCENTILE_TIME_TO_CONV, param)
        if val == "ttc_median":
            return (AggType.PERCENTILE_TIME_TO_CONV, 50)
        if val == "ttc_avg":
            return (AggType.AVERAGE_TIME_TO_CONV, None)
        if val == "conversion":
            return (AggType.CONVERSION, None)
        if val == "retention_rate":
            return (AggType.RETENTION_RATE, None)
        raise ValueError(
            f"Unsupported AggType: {val}\n"
            "supported['event_count', 'user_count', 'ttc_median', 'ttc_p90', 'ttc_p95',"
            " 'ttc_avg', 'conversion', 'retention']"
        )


class SimpleChartType(Enum):
    BAR = auto()
    STACKED_BAR = auto()
    LINE = auto()
    STACKED_AREA = auto()
    HEATMAP = auto()

    @classmethod
    def parse(cls, value: Union[str, SimpleChartType]) -> SimpleChartType:
        if type(value) == SimpleChartType:
            return value
        elif type(value) == str:
            for key, enm in SimpleChartType._member_map_.items():
                if key.lower() == value.lower():
                    return SimpleChartType[key]
            raise ValueError(
                f"Unknown chart type {value} supported are {[k.lower() for k in SimpleChartType._member_names_]}"
            )
        else:
            raise ValueError(
                f"Parse should only be str or SimpleChartType but it was {type(value)}"
            )


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
            return "like"
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

    def from_string(self, string_value: str) -> Any:
        if self == DataType.BOOL:
            return bool(string_value)
        if self == DataType.NUMBER:
            return float(string_value)
        if self == DataType.STRING:
            return string_value
        if self == DataType.DATETIME:
            try:
                return parser.parse(string_value)
            except parser.ParserError:
                return string_value
        else:
            raise Exception(f"Unsupported parsing for type: {self.name}.")


class AttributionMode(Enum):
    FIRST_EVENT = 1
    LAST_EVENT = 2
    ALL_EVENTS = 3


class ConnectionType(Enum):
    FILE = "file"
    ATHENA = "athena"
    TRINO = "trino"
    POSTGRESQL = "postgresql+psycopg2"
    MYSQL = "mysql+mysqlconnector"
    SQLITE = "sqlite"
    DATABRICKS = "databricks"
    SNOWFLAKE = "snowflake"

    @classmethod
    def parse(cls, val: str | ConnectionType) -> ConnectionType:
        if type(val) == str:
            val = val.upper()
            return ConnectionType[val]
        elif type(val) == ConnectionType:
            return val
        else:
            raise ValueError(
                f"Invalid argument type for ConnectionType parse: {type(val)}"
            )


@dataclass(frozen=True)
class TimeWindow:
    value: int = 1
    period: TimeGroup = TimeGroup.DAY

    @classmethod
    def parse(cls, val: str | TimeWindow) -> TimeWindow:
        if type(val) == str:
            vals = val.strip().split(" ")
            return TimeWindow(value=int(vals[0]), period=TimeGroup.parse(vals[1]))
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


class Identifiable(Protocol):
    def get_id(self) -> str:
        pass


T = TypeVar("T")
ID = TypeVar("ID", bound=Identifiable)


class InvalidReferenceException(Exception):
    pass


@dataclass
class State(Generic[T]):
    _val: Optional[T] = None

    def get_value(self) -> Optional[T]:
        return self._val

    def get_value_if_exsits(self) -> T:
        if self._val is None:
            raise ValueError("State is empty.")
        return self._val

    def set_value(self, value: Optional[T]):
        self._val = value

    def __getstate__(self):
        """Override so pickle doesn't store state"""
        return {}

    def __setstate__(self, state):
        """Override so pickle doesn't store state"""


@dataclass(init=False)
class Reference(Generic[ID]):
    """We need the Reference class to support multitenant Objects. E.g.
    If the Connection or Project objects are reused we don't want to store them in the DB
    multiple times. The reference protected _id must be the id of the object
    """

    _value_state: State[ID]
    _id: Optional[str]

    def __init__(self, value: Optional[ID]):
        self._value_state = State(value)
        if value is None:
            self._id = None
        else:
            self._id = value.get_id()

    def get_id(self) -> Optional[str]:
        return self._id

    def get_value(self) -> Optional[ID]:
        return self._value_state.get_value()

    def get_value_if_exists(self) -> ID:
        res = self.get_value()
        if res is None:
            raise InvalidReferenceException(
                f"Missing reference value for id: {self._id}"
            )
        return res

    def set_value(self, value: Optional[ID]):
        self._value_state.set_value(value)
        if value is None:
            self._id = None
        else:
            self._id = value.get_id()

    def restore_value(self, value: Optional[ID]):
        if self._id is None:
            raise InvalidReferenceException("Restoring reference without ID.")
        self._value_state.set_value(value)
        if value is not None and value.get_id() != self._id:
            raise InvalidReferenceException(
                "Restored reference value has different ID."
            )


class SecretResolver(ABC):
    """
    Abstract base class for all secret resolvers
    """

    def resolve_secret(self) -> str:
        raise NotImplementedError()


@dataclass(frozen=True)
class PromptSecretResolver(SecretResolver):
    """
    Prompts for a secret value on the first query execution in an interactive Python environment.

    :param title: the title of the prompt
    """

    title: str = "Secret"

    def resolve_secret(self) -> str:
        import getpass

        return getpass.getpass(prompt=self.title)


@dataclass(frozen=True)
class ConstSecretResolver(SecretResolver):
    """
    Resolves a secret with a preconfigured static value.

    :param secret: the secret value
    """

    secret: str

    def resolve_secret(self) -> str:
        return self.secret


@dataclass(frozen=True)
class EnvVarSecretResolver(SecretResolver):
    """
    Resolves a secret from an environmental variable or raises an exception if the environment variable is not found.

    :param variable_name: name of the environment variable with containing the secret
    """

    variable_name: str

    def resolve_secret(self) -> str:
        secret = os.getenv(self.variable_name)
        if secret is not None:
            return secret
        else:
            raise Exception(f"Environmental variable {self.variable_name} was not set.")


@dataclass(frozen=True)
class Connection(Identifiable):
    """
    Contains all details needed to connect to a data warehouse.

    :param connection_type: type of the connection
    :param user_name: username
    :param secret_resolver: secret resolver to get the user password
    :param host: hostname
    :param port: port number
    :param schema: schema name
    :param catalog: catalog name
    :param url_params: extra sqlite connection url parameters
    :param extra_configs: used for connection adapter configuration
    """

    connection_name: str
    connection_type: ConnectionType
    id: str = field(default_factory=helper.create_unique_id)
    user_name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    url: Optional[str] = None
    # TBD remove schema
    schema: Optional[str] = None
    catalog: Optional[str] = None
    # Used for connection url parametrization
    url_params: Optional[str] = None
    # Used for adapter configuration
    extra_configs: Dict[str, Any] = field(default_factory=dict)
    _secret: State[str] = field(default_factory=State)
    secret_resolver: Optional[SecretResolver] = None

    @property
    def password(self):
        if self._secret.get_value() is None:
            if self.secret_resolver is not None:
                secret = self.secret_resolver.resolve_secret()
                self._secret.set_value(secret)
            else:
                return ""
        return self._secret.get_value()

    def get_url_param(self, param: str) -> Optional[str]:
        res = parse.parse_qs(self.url_params).get(param)
        if res is not None and len(res) == 1:
            return res[0]
        return None

    def get_id(self) -> str:
        return self.id


@dataclass(frozen=True)
class DiscoverySettings:
    max_enum_cardinality: int = 300
    max_map_key_cardinality: int = 1000

    end_dt: Optional[datetime] = None
    property_sample_rate: int = 0

    lookback_days: int = 14
    min_property_sample_size: int = 1000


class WebappEndDateConfig(Enum):
    CUSTOM_DATE = auto()
    NOW = auto()
    START_OF_CURRENT_DAY = auto()
    END_OF_CURRENT_DAY = auto()


@dataclass(frozen=True)
class WebappSettings:
    lookback_window: TimeWindow = TimeWindow(30, TimeGroup.DAY)
    auto_refresh_enabled: bool = True
    end_date_config: WebappEndDateConfig = WebappEndDateConfig.START_OF_CURRENT_DAY
    custom_end_date: Optional[datetime] = None


class InvalidEventDataTableError(Exception):
    pass


@dataclass(unsafe_hash=False)
class EventDataTable(Identifiable):
    """
    Refers to a single table in the data warehouse or data lake.
    """

    table_name: str
    event_time_field: Field
    user_id_field: Field

    id: str = field(default_factory=helper.create_unique_id)
    schema: Optional[str] = None
    catalog: Optional[str] = None
    event_name_field: Optional[Field] = None
    event_name_alias: Optional[str] = None
    date_partition_field: Optional[Field] = None

    # TBD change to Field type
    ignored_fields: List[str] = field(default_factory=lambda: [])

    # TBD change to Field type
    event_specific_fields: Optional[List[str]] = None

    # TBD remove
    description: Optional[str] = None

    discovery_settings: Optional[DiscoverySettings] = None
    project_reference: Reference[Project] = field(
        default_factory=lambda: Reference(None)
    )

    @classmethod
    def create(
        cls,
        table_name: str,
        event_time_field: str,
        user_id_field: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        event_name_field: Optional[str] = None,
        event_name_alias: Optional[str] = None,
        ignored_fields: List[str] = None,
        event_specific_fields: List[str] = None,
        date_partition_field: Optional[str] = None,
        description: str = None,
        discovery_settings: Optional[DiscoverySettings] = None,
    ):

        if event_name_field == "":
            event_name_field = None
        if not event_name_field and event_name_alias is None:
            event_name_alias = table_name

        return EventDataTable(
            table_name=table_name,
            event_name_alias=event_name_alias,
            description=description,
            ignored_fields=([] if ignored_fields is None else ignored_fields),
            event_specific_fields=event_specific_fields,
            event_name_field=Field(_name=event_name_field, _type=DataType.STRING)
            if event_name_field is not None
            else None,
            date_partition_field=Field(
                _name=date_partition_field, _type=DataType.DATETIME
            )
            if date_partition_field is not None
            else None,
            event_time_field=Field(_name=event_time_field, _type=DataType.DATETIME),
            user_id_field=Field(_name=user_id_field, _type=DataType.STRING),
            schema=schema,
            catalog=catalog,
            discovery_settings=discovery_settings,
        )

    @classmethod
    def single_event_table(
        cls,
        table_name: str,
        event_time_field: str,
        user_id_field: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        event_name_alias: Optional[str] = None,
        ignored_fields: List[str] = None,
        date_partition_field: Optional[str] = None,
        description: str = None,
        discovery_settings: Optional[DiscoverySettings] = None,
    ):
        """
        Creates an Event Data Table from a table in a data warehouse

        :param table_name: name of the table
        :param event_time_field: name of the field containing the event time
        :param user_id_field: name of the field containing the user ID
        :param schema:
        :param catalog:
        :param event_name_alias:
        :param ignored_fields: name of the field which should be ignored
        :param date_partition_field: name of the field used for partitioning the data by date
        :param description:
        :param discovery_settings: discovery settings, if None then the project wide discovery settings will be used
        """
        return EventDataTable.create(
            table_name=table_name,
            event_name_alias=event_name_alias,
            description=description,
            ignored_fields=ignored_fields,
            event_specific_fields=None,
            event_name_field=None,
            date_partition_field=date_partition_field,
            event_time_field=event_time_field,
            user_id_field=user_id_field,
            schema=schema,
            catalog=catalog,
            discovery_settings=discovery_settings,
        )

    @classmethod
    def multi_event_table(
        cls,
        table_name: str,
        event_time_field: str,
        user_id_field: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
        event_name_field: Optional[str] = None,
        ignored_fields: List[str] = None,
        event_specific_fields: List[str] = None,
        date_partition_field: Optional[str] = None,
        description: str = None,
        discovery_settings: Optional[DiscoverySettings] = None,
    ):
        """
        Creates an Event Data Table from a table in a data warehouse

        :param table_name: name of the table
        :param event_time_field: name of the field containing the event time
        :param user_id_field: name of the field containing the user ID
        :param schema:
        :param catalog:
        :param event_name_alias:
        :param ignored_fields: name of the field which should be ignored
        :param event_specific_fields: name of the fields which are specific for certain events,
            these fields will be discovered separately for every event.
        :param date_partition_field: name of the field used for partitioning the data by date
        :param description:
        :param discovery_settings: discovery settings, if None then the project wide discovery settings will be used
        """
        return EventDataTable.create(
            table_name=table_name,
            event_name_alias=None,
            description=description,
            ignored_fields=ignored_fields,
            event_specific_fields=event_specific_fields,
            event_name_field=event_name_field,
            date_partition_field=date_partition_field,
            event_time_field=event_time_field,
            user_id_field=user_id_field,
            schema=schema,
            catalog=catalog,
            discovery_settings=discovery_settings,
        )

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        if isinstance(other, EventDataTable):
            return self.id == other.id
        return False

    def get_id(self) -> str:
        return self.id

    def get_full_name(self) -> str:
        schema = "" if self.schema is None else self.schema + "."
        catalog = "" if self.catalog is None else self.catalog + "."
        return f"{catalog}{schema}{self.table_name}"

    def update_discovery_settings(self, discovery_settings: DiscoverySettings):
        properties = self.__dict__.copy()
        properties.update(discovery_settings=discovery_settings)
        return EventDataTable(**properties)

    def validate(self, adapter: GA.GenericDatasetAdapter):
        if self.event_name_alias is not None and self.event_name_field is not None:
            raise InvalidEventDataTableError(
                f"For {self.table_name} both event_name_alias and event_name_field can't be defined in the same time."
            )
        if self.event_name_alias is None and self.event_name_field is None:
            raise InvalidEventDataTableError(
                f"For {self.table_name} define the event_name_alias or the event_name_field property."
            )

        available_fields = [f._get_name().lower() for f in adapter.list_fields(self)]
        if len(available_fields) == 0:
            raise InvalidEventDataTableError(
                f"No fields in event data table '{self.table_name}'"
            )

        for field_to_validate in [
            self.event_name_field,
            self.event_time_field,
            self.user_id_field,
            self.date_partition_field,
        ]:
            if field_to_validate is None:
                continue

            if field_to_validate._get_name().lower() not in available_fields:
                raise InvalidEventDataTableError(
                    f"Event data table '{self.table_name}' does not have '{field_to_validate._get_name()}' field"
                )

    @property
    def project(self) -> Project:
        res = self.project_reference.get_value()
        if res is None:
            raise InvalidReferenceException("EventDataTable doesn't have a Project")
        return res

    def set_project(self, project: Project):
        self.project_reference = Reference(project)


class InvalidProjectError(Exception):
    pass


@dataclass(init=False)
class Project(Identifiable):
    """
    Defines a Mitzu project

    :param connection: configurations to connect to a data warehouse
    :param event_data_tables: list of Event Data Tables containing the events
    :param discovery_settings: default, project wide discovery settings
    :param webapp_settings: configurations to represent the project in the Mitzu webapp
    """

    project_name: str
    event_data_tables: List[EventDataTable]
    discovery_settings: DiscoverySettings
    webapp_settings: WebappSettings
    description: Optional[str]
    id: str
    _connection_ref: Reference[Connection]
    _adapter_cache: State[GA.GenericDatasetAdapter]
    _discovered_project: State[DiscoveredProject]

    def __init__(
        self,
        connection: Connection,
        event_data_tables: List[EventDataTable],
        project_name: str,
        project_id: Optional[str] = None,
        description: Optional[str] = None,
        discovery_settings: Optional[DiscoverySettings] = None,
        webapp_settings: Optional[WebappSettings] = None,
    ):
        object.__setattr__(self, "id", project_id)
        edt_with_discovery_settings = []
        if discovery_settings is None:
            discovery_settings = DiscoverySettings()
        for edt in event_data_tables:
            edt.set_project(self)
            if edt.discovery_settings is None:
                edt_with_discovery_settings.append(
                    edt.update_discovery_settings(discovery_settings)
                )
            else:
                edt_with_discovery_settings.append(edt)

        if webapp_settings is None:
            webapp_settings = WebappSettings()

        if project_id is None:
            project_id = helper.create_unique_id()

        object.__setattr__(self, "event_data_tables", edt_with_discovery_settings)
        object.__setattr__(self, "discovery_settings", discovery_settings)
        object.__setattr__(self, "webapp_settings", webapp_settings)
        object.__setattr__(self, "project_name", project_name)
        object.__setattr__(self, "description", description)
        object.__setattr__(self, "_connection_ref", Reference(connection))
        object.__setattr__(self, "_adapter_cache", State())
        object.__setattr__(self, "_discovered_project", State())

    def get_adapter(self) -> GA.GenericDatasetAdapter:
        val = self._adapter_cache.get_value()
        if val is None:
            adp = factory.create_adapter(self)
            self._adapter_cache.set_value(adp)
            return adp
        else:
            return val

    @property
    def connection(self) -> Connection:
        res = self._connection_ref.get_value()
        if res is None:
            raise InvalidReferenceException("Connection is missing from the Project")
        return res

    def get_connection_id(self) -> str:
        res = self._connection_ref.get_id()
        if res is None:
            raise InvalidReferenceException("Project has not Connection ID")
        return res

    def set_connection(self, connection: Optional[Connection]):
        self._connection_ref.set_value(connection)

    def restore_connection(self, connection: Optional[Connection]):
        self._connection_ref.restore_value(connection)

    def get_default_end_dt(self) -> datetime:
        if (
            self.webapp_settings.end_date_config == WebappEndDateConfig.CUSTOM_DATE
            and self.webapp_settings.custom_end_date is not None
        ):
            return self.webapp_settings.custom_end_date
        if (
            self.webapp_settings.end_date_config
            == WebappEndDateConfig.START_OF_CURRENT_DAY
        ):
            return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if (
            self.webapp_settings.end_date_config
            == WebappEndDateConfig.END_OF_CURRENT_DAY
        ):
            return datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
        return datetime.now()

    def get_default_discovery_start_dt(self) -> datetime:
        return self.get_default_end_dt() - timedelta(
            days=self.discovery_settings.lookback_days
        )

    def discover_project(
        self, progress_bar: bool = True, callback: Optional[Callable] = None
    ) -> DiscoveredProject:
        """
        Discovers all Event Data Tables with the given discovery settings.

        :param progress_bar: if True then a progressbar will be shown
        :param callback: callback function for each EDT discovered
        :return: DiscoveredProject containing all the discovered event properties
        """
        return D.ProjectDiscovery(project=self, callback=callback).discover_project(
            progress_bar
        )

    def validate(self):
        if len(self.event_data_tables) == 0:
            raise InvalidProjectError(
                "At least a single EventDataTable needs to be added to the Project.\n"
                "Project(event_data_tables = [ EventDataTable.create(...)])"
            )
        try:
            self.get_adapter().test_connection()
        except Exception as e:
            raise InvalidProjectError(f"Connection failed: {str(e)}") from e

        for edt in self.event_data_tables:
            edt.validate(self.get_adapter())

    def get_id(self) -> str:
        return self.id


class DatasetModel:
    @classmethod
    def _to_globals(cls, glbs: Dict):
        for k, v in cls.__dict__.items():
            if k != "_to_globals":
                glbs[k] = v


@dataclass(frozen=True, init=False)
class DiscoveredProject:
    definitions: Dict[EventDataTable, Dict[str, Reference[EventDef]]]
    project: Project

    def __init__(
        self,
        definitions: Dict[EventDataTable, Dict[str, Reference[EventDef]]],
        project: Project,
    ) -> None:
        object.__setattr__(self, "definitions", definitions)
        object.__setattr__(self, "project", project)
        project._discovered_project.set_value(self)

    def __post_init__(self):
        self.project.validate()

    def create_notebook_class_model(self) -> Any:
        return ML.ModelLoader().create_datasource_class_model(self)

    def display_inline_dashboard(
        self,
        port: int = 8080,
        host: str = "0.0.0.0",
        external_url: Optional[str] = None,
        logging_level: int = logging.WARN,
        height: Any = 800,
        width: Any = "100%",
    ):
        import IPython.display as ID

        if not external_url:
            external_url = f"http://{host}:{port}"
        results = self.display_external_dashboard(
            port, host, logging_level, new_thread=True
        )
        ID.display(ID.IFrame(external_url, height=height, width=width))
        return results

    def display_external_dashboard(
        self,
        port: Optional[int] = 8080,
        host: Optional[str] = "0.0.0.0",
        logging_level: int = logging.WARN,
        new_thread: bool = False,
    ):
        import mitzu.webapp.notebook as DASH

        self.project.validate()
        results: Optional[Dict[str, Any]] = {}
        DASH.external_dashboard(
            self,
            results=results,
            port=port,
            host=host,
            logging_level=logging_level,
            new_thread=new_thread,
        )
        return results

    def get_event_def(self, event_name) -> EventDef:
        for val in self.definitions.values():
            res = val.get(event_name)
            if res is not None:
                return res.get_value_if_exists()
        raise Exception(
            f"Invalid state, {event_name} is not present in Discovered Datasource."
        )

    def get_all_events(self) -> Dict[str, EventDef]:
        res: Dict[str, EventDef] = {}
        for val in self.definitions.values():
            res = {**res, **{k: v.get_value_if_exists() for k, v in val.items()}}
        return res

    def get_all_event_names(self) -> List[str]:
        res: List[str] = []
        for val in self.definitions.values():
            res.extend(val.keys())
        return res

    @staticmethod
    def _get_path(
        project_name: str, folder: str = "./", extension="mitzu"
    ) -> pathlib.Path:
        if project_name.endswith(f".{extension}"):
            return pathlib.Path(folder, f"{project_name}")
        else:
            return pathlib.Path(folder, f"{project_name}.{extension}")

    def save_to_project_file(
        self, project_name: str, folder: str = "./", extension="mitzu"
    ):
        path = self._get_path(project_name, folder, extension)
        with path.open(mode="w") as file:
            file.write(PSE.serialize_discovered_project(self))

    @classmethod
    def load_from_project_file(
        cls, project_name: str, folder: str = "./", extension="mitzu"
    ) -> DiscoveredProject:
        path = cls._get_path(project_name, folder, extension)
        with path.open(mode="rb") as file:
            return PSE.deserialize_discovered_project(file.read())


@dataclass(frozen=True, init=False)
class Field:
    _name: str
    _type: DataType = field(repr=False)
    _sub_fields: Optional[Tuple[Field, ...]] = field(hash=False, compare=False)
    _parent: Optional[Field] = field(
        repr=False,
        default=None,
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

    def get_all_subfields(self) -> List[Field]:
        if self._sub_fields is None:
            return [self]
        else:
            all_sfs = []
            for sf in self._sub_fields:
                all_sfs.extend(sf.get_all_subfields())
            return all_sfs

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
    _event_data_table: EventDataTable
    _description: Optional[str] = ""
    _enums: Optional[List[Any]] = None

    @property
    def _project(self) -> Project:
        return self._event_data_table.project


@dataclass(frozen=True)
class EventDef(Identifiable):
    _event_name: str
    _fields: Dict[Field, EventFieldDef]
    _event_data_table: EventDataTable
    _description: Optional[str] = ""

    @property
    def _project(self) -> Project:
        return self._event_data_table.project

    def get_id(self) -> str:
        return self._event_data_table.id + f"_{self._event_name}"


# =========================================== Metric definitions ===========================================

DEF_MAX_GROUP_COUNT = 10
DEF_LOOK_BACK_DAYS = TimeWindow(30, TimeGroup.DAY)
DEF_CONV_WINDOW = TimeWindow(1, TimeGroup.DAY)
DEF_RET_WINDOW = TimeWindow(1, TimeGroup.WEEK)
DEF_TIME_GROUP = TimeGroup.DAY


class Resolution(Enum):
    EVERY_EVENT = auto()
    ONE_USER_EVENT_PER_MINUTE = auto()
    ONE_USER_EVENT_PER_HOUR = auto()
    ONE_USER_EVENT_PER_DAY = auto()

    @classmethod
    def parse(cls, val: str | Resolution) -> Resolution:
        if type(val) == str:
            val = val.upper()
            return Resolution[val]
        elif type(val) == Resolution:
            return val
        else:
            raise ValueError(f"Invalid argument type for Resolution parse: {type(val)}")

    def get_time_group(self) -> Optional[TimeGroup]:
        if self == Resolution.ONE_USER_EVENT_PER_DAY:
            return TimeGroup.DAY
        if self == Resolution.ONE_USER_EVENT_PER_HOUR:
            return TimeGroup.HOUR
        if self == Resolution.ONE_USER_EVENT_PER_MINUTE:
            return TimeGroup.MINUTE
        return None


@dataclass()
class MetricConfig:
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    lookback_days: TimeWindow = DEF_LOOK_BACK_DAYS
    time_group: TimeGroup = DEF_TIME_GROUP
    max_group_count: int = DEF_MAX_GROUP_COUNT
    group_by: Optional[EventFieldDef] = None
    custom_title: Optional[str] = None
    agg_type: Optional[AggType] = None
    agg_param: Optional[Any] = None
    chart_type: Optional[SimpleChartType] = None
    resolution: Resolution = Resolution.EVERY_EVENT


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
        return self._config.lookback_days

    @property
    def _time_group(self) -> TimeGroup:
        return self._config.time_group

    @property
    def _resolution(self) -> Resolution:
        return self._config.resolution

    @property
    def _group_by(self) -> Optional[EventFieldDef]:
        return self._config.group_by

    @property
    def _chart_type(self) -> Optional[SimpleChartType]:
        return self._config.chart_type

    @property
    def _custom_title(self) -> Optional[str]:
        return self._config.custom_title

    @property
    def _end_dt(self) -> datetime:
        if self._config.end_dt is not None:
            return self._config.end_dt
        return self.get_project().get_default_end_dt()

    @property
    def _start_dt(self) -> datetime:
        if self._config.start_dt is not None:
            return self._config.start_dt
        return self._end_dt - self._lookback_days.to_relative_delta()

    @property
    def _agg_type(self) -> AggType:
        if self._config.agg_type is not None:
            return self._config.agg_type
        if isinstance(self, ConversionMetric):
            return AggType.CONVERSION
        if isinstance(self, SegmentationMetric):
            return AggType.COUNT_UNIQUE_USERS
        if isinstance(self, RetentionMetric):
            return AggType.RETENTION_RATE
        raise NotImplementedError(
            f"_agg_type property is not implemented for {type(self)}"
        )

    @property
    def _agg_param(self) -> Any:
        if self._config.agg_param is not None:
            return self._config.agg_param
        if self._agg_type == AggType.PERCENTILE_TIME_TO_CONV:
            return 50
        return None

    def get_project(self) -> Project:
        raise NotImplementedError()

    def get_df(self) -> pd.DataFrame:
        raise NotImplementedError()

    def get_sql(self) -> pd.DataFrame:
        raise NotImplementedError()

    def get_title(self) -> str:
        raise NotImplementedError()

    def print_sql(self):
        print(self.get_sql())

    def get_figure(self):
        chart = CHRT.get_simple_chart(self)
        return PLT.plot_chart(chart, self)

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
        project = helper.get_segment_project(self._conversion._segments[0])
        return project.get_adapter().get_conversion_df(self)

    def get_sql(self) -> pd.DataFrame:
        project = helper.get_segment_project(self._conversion._segments[0])
        return project.get_adapter().get_conversion_sql(self)

    def get_figure(self):
        chart = CHRT.get_simple_chart(self)
        return PLT.plot_chart(chart, self)

    def __repr__(self) -> str:
        return super().__repr__()

    def get_project(self) -> Project:
        curr: Segment = self._conversion._segments[0]
        while not isinstance(curr, SimpleSegment):
            curr = cast(ComplexSegment, curr)._left
        return curr._left._project

    def get_title(self) -> str:
        return TI.get_conversion_title(self)


@dataclass(frozen=True, init=False)
class SegmentationMetric(Metric):
    _segment: Segment

    def __init__(self, segment: Segment, config: MetricConfig):
        super().__init__(config)
        object.__setattr__(self, "_segment", segment)

    def get_df(self) -> pd.DataFrame:
        project = helper.get_segment_project(self._segment)
        return project.get_adapter().get_segmentation_df(self)

    def get_sql(self) -> str:
        project = helper.get_segment_project(self._segment)
        return project.get_adapter().get_segmentation_sql(self)

    def __repr__(self) -> str:
        return super().__repr__()

    def get_project(self) -> Project:
        curr: Segment = self._segment
        while not isinstance(curr, SimpleSegment):
            curr = cast(ComplexSegment, curr)._left
        return curr._left._project

    def get_title(self) -> str:
        return TI.get_segmentation_title(self)


@dataclass(frozen=True, init=False)
class RetentionMetric(Metric):

    _initial_segment: Segment
    _retaining_segment: Segment
    _retention_window: TimeWindow

    def __init__(
        self,
        initial_segment: Segment,
        retaining_segment: Segment,
        retention_window: TimeWindow,
        config: MetricConfig,
    ):
        super().__init__(config)
        object.__setattr__(self, "_initial_segment", initial_segment)
        object.__setattr__(self, "_retaining_segment", retaining_segment)
        object.__setattr__(self, "_retention_window", retention_window)

    def config(
        self,
        start_dt: Optional[str | datetime] = None,
        end_dt: Optional[str | datetime] = None,
        custom_title: Optional[str] = None,
        retention_window: Union[TimeWindow, str] = TimeWindow(
            value=1, period=TimeGroup.WEEK
        ),
        time_group: Union[str, TimeGroup] = TimeGroup.WEEK,
        group_by: Optional[EventFieldDef] = None,
        max_group_by_count: int = DEF_MAX_GROUP_COUNT,
        lookback_days: Union[int, TimeWindow] = DEF_LOOK_BACK_DAYS,
        chart_type: Optional[Union[str, SimpleChartType]] = None,
        resolution: Union[str, Resolution] = Resolution.EVERY_EVENT,
    ) -> RetentionMetric:
        chart_type = chart_type
        if type(chart_type) == str:
            chart_type = SimpleChartType.parse(chart_type)

        config = MetricConfig(
            start_dt=helper.parse_datetime_input(start_dt, None),
            end_dt=helper.parse_datetime_input(end_dt, None),
            time_group=(
                time_group
                if type(time_group) == TimeGroup
                else TimeGroup.parse(time_group)
            ),
            custom_title=custom_title,
            group_by=group_by,
            max_group_count=max_group_by_count,
            lookback_days=(
                lookback_days
                if type(lookback_days) == TimeWindow
                else TimeWindow(cast(int, lookback_days), TimeGroup.DAY)
            ),
            agg_type=AggType.RETENTION_RATE,
            agg_param=None,
            resolution=(
                resolution
                if type(resolution) == Resolution
                else Resolution.parse(resolution)
            ),
            chart_type=cast(SimpleChartType, chart_type),
        )

        return RetentionMetric(
            initial_segment=self._initial_segment,
            retaining_segment=self._retaining_segment,
            retention_window=(
                retention_window
                if type(retention_window) == TimeWindow
                else TimeWindow.parse(retention_window)
            ),
            config=config,
        )

    def __repr__(self) -> str:
        return super().__repr__()

    def get_df(self) -> pd.DataFrame:
        project = helper.get_segment_project(self._initial_segment)
        return project.get_adapter().get_retention_df(self)

    def get_sql(self) -> str:
        project = helper.get_segment_project(self._initial_segment)
        return project.get_adapter().get_retention_sql(self)

    def get_project(self) -> Project:
        curr: Segment = self._initial_segment
        while not isinstance(curr, SimpleSegment):
            curr = cast(ComplexSegment, curr)._left
        return curr._left._project

    def get_title(self) -> str:
        return TI.get_retention_title(self)


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
        conv_window: Optional[Union[str, TimeWindow]] = DEF_CONV_WINDOW,
        start_dt: Optional[Union[str, datetime]] = None,
        end_dt: Optional[Union[str, datetime]] = None,
        time_group: Union[str, TimeGroup] = DEF_TIME_GROUP,
        group_by: Optional[EventFieldDef] = None,
        max_group_by_count: int = DEF_MAX_GROUP_COUNT,
        lookback_days: Union[int, TimeWindow] = DEF_LOOK_BACK_DAYS,
        custom_title: Optional[str] = None,
        aggregation: Union[str, AggType] = AggType.CONVERSION,
        chart_type: Optional[Union[str, SimpleChartType]] = None,
        resolution: Union[str, Resolution] = Resolution.EVERY_EVENT,
    ) -> ConversionMetric:
        if type(lookback_days) == int:
            lookback_days = TimeWindow(lookback_days, TimeGroup.DAY)

        agg_param = None
        if type(aggregation) != AggType:
            agg_type, agg_param = AggType.parse_agg_str(cast(str, aggregation))
        else:
            agg_type = aggregation

        chart_type = chart_type
        if type(chart_type) == str:
            chart_type = SimpleChartType.parse(chart_type)

        config = MetricConfig(
            start_dt=helper.parse_datetime_input(start_dt, None),
            end_dt=helper.parse_datetime_input(end_dt, None),
            time_group=(
                time_group
                if type(time_group) == TimeGroup
                else TimeGroup.parse(time_group)
            ),
            custom_title=custom_title,
            group_by=group_by,
            max_group_count=max_group_by_count,
            lookback_days=(
                lookback_days
                if type(lookback_days) == TimeWindow
                else TimeWindow(cast(int, lookback_days), TimeGroup.DAY)
            ),
            agg_type=agg_type,
            agg_param=agg_param,
            resolution=(
                resolution
                if type(resolution) == Resolution
                else Resolution.parse(resolution)
            ),
            chart_type=cast(SimpleChartType, chart_type),
        )

        if conv_window is not None:
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

    def __ge__(self, retaining_segment: Segment) -> RetentionMetric:
        return RetentionMetric(
            self,
            retaining_segment,
            retention_window=TimeWindow(value=1, period=TimeGroup.WEEK),
            config=MetricConfig(time_group=TimeGroup.WEEK),
        )

    def config(
        self,
        start_dt: Optional[Union[str, datetime]] = None,
        end_dt: Optional[Union[str, datetime]] = None,
        time_group: Union[str, TimeGroup] = DEF_TIME_GROUP,
        group_by: Optional[EventFieldDef] = None,
        max_group_by_count: int = DEF_MAX_GROUP_COUNT,
        lookback_days: Union[int, TimeWindow] = DEF_LOOK_BACK_DAYS,
        custom_title: Optional[str] = None,
        aggregation: Union[str, AggType] = AggType.COUNT_UNIQUE_USERS,
        chart_type: Optional[Union[str, SimpleChartType]] = None,
    ) -> SegmentationMetric:
        agg_param = None
        if type(aggregation) != AggType:
            agg_type, agg_param = AggType.parse_agg_str(cast(str, aggregation))
        else:
            agg_type = aggregation

        chart_type = chart_type
        if type(chart_type) == str:
            chart_type = SimpleChartType.parse(chart_type)

        config = MetricConfig(
            start_dt=helper.parse_datetime_input(start_dt, None),
            end_dt=helper.parse_datetime_input(end_dt, None),
            time_group=(
                time_group
                if type(time_group) == TimeGroup
                else TimeGroup.parse(time_group)
            ),
            custom_title=custom_title,
            group_by=group_by,
            max_group_count=max_group_by_count,
            lookback_days=(
                lookback_days
                if type(lookback_days) == TimeWindow
                else TimeWindow(cast(int, lookback_days), TimeGroup.DAY)
            ),
            agg_type=agg_type,
            agg_param=agg_param,
            resolution=Resolution.EVERY_EVENT,
            chart_type=cast(SimpleChartType, chart_type),
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

    def __hash__(self) -> int:
        return hash(f"{hash(self._left)}{self._operator}{hash(self._right)}")


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

    def __hash__(self) -> int:
        event_property_name = (
            self._left._event_name
            if type(self._left) != EventFieldDef
            else f"{self._left._event_name}.{self._left._field._get_name()}"
        )
        return hash(f"{event_property_name}{self._operator}{self._right}")
