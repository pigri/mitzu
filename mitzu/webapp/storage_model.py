from __future__ import annotations

from typing import List, Optional, Dict, Any
import json
import base64
import pickle
import io

import mitzu.model as M
import mitzu.webapp.model as WM
import mitzu.visualization.common as VC
import sqlalchemy as SA

Base: SA.orm.DeclarativeMeta = SA.orm.declarative_base()


def serialize_field(field: M.Field) -> str:
    return json.dumps(
        {
            "_name": field._name,
            "_type": field._type.value,
            "_sub_fields": [serialize_field(f) for f in field._sub_fields]
            if field._sub_fields
            else None,
            "_parent": serialize_field(field._parent) if field._parent else None,
        }
    )


def deserialize_field(serialized: str) -> M.Field:
    data = json.loads(serialized)
    return M.Field(
        _name=data["_name"],
        _type=M.DataType(data["_type"]),
        _sub_fields=data["_sub_fields"],
        _parent=data["_parent"],
    )


class UserStorageRecord(Base):
    __tablename__ = "users"

    user_id = SA.Column(SA.String, primary_key=True)
    email = SA.Column(SA.String)
    password_hash = SA.Column(SA.String)
    password_salt = SA.Column(SA.String)
    role = SA.Column(SA.String)

    def update(self, user: WM.User):
        self.email = user.email
        self.password_hash = user.password_hash
        self.password_salt = user.password_salt
        self.role = user.role.value

    def as_model_instance(self) -> WM.User:
        return WM.User(
            id=self.user_id,
            email=self.email,
            password_hash=self.password_hash,
            password_salt=self.password_salt,
            role=WM.Role(self.role),
        )

    @classmethod
    def from_model_instance(self, model: WM.User) -> UserStorageRecord:
        return UserStorageRecord(
            user_id=model.id,
            email=model.email,
            password_hash=model.password_hash,
            password_salt=model.password_salt,
            role=model.role.value,
        )


class DiscoverySettingsStorageRecord(Base):
    __tablename__ = "discovery_settings"

    discovery_settings_id = SA.Column(SA.String, primary_key=True)
    max_enum_cardinality = SA.Column(SA.Integer)
    max_map_key_cardinality = SA.Column(SA.Integer)
    end_dt = SA.Column(SA.DateTime, nullable=True)
    property_sample_rate = SA.Column(SA.Integer)
    lookback_days = SA.Column(SA.Integer)
    min_property_sample_size = SA.Column(SA.Integer)

    def update(self, settings: M.DiscoverySettings):
        self.max_enum_cardinality = settings.max_enum_cardinality
        self.max_map_key_cardinality = settings.max_map_key_cardinality

        self.end_dt = settings.end_dt
        self.property_sample_rate = settings.property_sample_rate

        self.lookback_days = settings.lookback_days
        self.min_property_sample_size = settings.min_property_sample_size

    def as_model_instance(self) -> M.DiscoverySettings:
        return M.DiscoverySettings(
            id=self.discovery_settings_id,
            max_enum_cardinality=self.max_enum_cardinality,
            max_map_key_cardinality=self.max_map_key_cardinality,
            end_dt=self.end_dt,
            property_sample_rate=self.property_sample_rate,
            lookback_days=self.lookback_days,
            min_property_sample_size=self.min_property_sample_size,
        )

    @classmethod
    def from_model_instance(
        self, settings: M.DiscoverySettings
    ) -> DiscoverySettingsStorageRecord:
        return DiscoverySettingsStorageRecord(
            discovery_settings_id=settings.id,
            max_enum_cardinality=settings.max_enum_cardinality,
            max_map_key_cardinality=settings.max_map_key_cardinality,
            end_dt=settings.end_dt,
            property_sample_rate=settings.property_sample_rate,
            lookback_days=settings.lookback_days,
            min_property_sample_size=settings.min_property_sample_size,
        )


class WebappSettingsStorageRecord(Base):
    __tablename__ = "webapp_settings"

    webapp_settings_id = SA.Column(SA.String, primary_key=True)
    lookback_window_value = SA.Column(SA.Integer)
    lookback_window_period = SA.Column(SA.String)
    auto_refresh_enabled = SA.Column(SA.Boolean)
    end_date_config = SA.Column(SA.String)
    custom_end_date = SA.Column(SA.DateTime, nullable=True)

    def update(self, settings: M.WebappSettings):
        self.lookback_window_value = settings.lookback_window.value
        self.lookback_window_period = str(settings.lookback_window.period)
        self.auto_refresh_enabled = settings.auto_refresh_enabled
        self.end_date_config = str(settings.end_date_config)
        self.custom_end_date = settings.custom_end_date

    def as_model_instance(self) -> M.WebappSettings:
        return M.WebappSettings(
            id=self.webapp_settings_id,
            lookback_window=M.TimeWindow(
                self.lookback_window_value,
                M.TimeGroup.parse(self.lookback_window_period),
            ),
            auto_refresh_enabled=self.auto_refresh_enabled,
            end_date_config=M.WebappEndDateConfig.parse(self.end_date_config),
            custom_end_date=self.custom_end_date,
        )

    @classmethod
    def from_model_instance(
        self, settings: M.WebappSettings
    ) -> WebappSettingsStorageRecord:
        return WebappSettingsStorageRecord(
            webapp_settings_id=settings.id,
            lookback_window_value=settings.lookback_window.value,
            lookback_window_period=str(settings.lookback_window.period),
            auto_refresh_enabled=settings.auto_refresh_enabled,
            end_date_config=str(settings.end_date_config),
            custom_end_date=settings.custom_end_date,
        )


class ConnectionStorageRecord(Base):
    __tablename__ = "connections"

    connection_id = SA.Column(SA.String, primary_key=True)
    connection_name = SA.Column(SA.String)
    connection_type = SA.Column(SA.Integer)

    user_name = SA.Column(SA.String, nullable=True, default=None)
    host = SA.Column(SA.String, nullable=True, default=None)
    port = SA.Column(SA.Integer, nullable=True, default=None)
    url = SA.Column(SA.String, nullable=True, default=None)

    schema = SA.Column(SA.String, nullable=True, default=None)
    catalog = SA.Column(SA.String, nullable=True, default=None)

    url_params = SA.Column(SA.String, nullable=True, default=None)

    extra_configs = SA.Column(SA.String, nullable=True, default=None)

    secret_resolver_type = SA.Column(SA.String, nullable=True, default=None)
    secret_resolver_arg = SA.Column(SA.String, nullable=True, default=None)

    def update(self, connection: M.Connection):
        secret_resolver_type = None
        secret_resolver_arg = None
        if isinstance(connection.secret_resolver, M.EnvVarSecretResolver):
            secret_resolver_type = "env"
            secret_resolver_arg = connection.secret_resolver.variable_name
        elif isinstance(connection.secret_resolver, M.ConstSecretResolver):
            secret_resolver_type = "const"
            secret_resolver_arg = connection.secret_resolver.resolve_secret()
        elif isinstance(connection.secret_resolver, M.PromptSecretResolver):
            secret_resolver_type = "prompt"
            secret_resolver_arg = connection.secret_resolver.title

        self.connection_name = connection.connection_name
        self.connection_type = connection.connection_type.value
        self.user_name = connection.user_name
        self.host = connection.host
        self.port = connection.port
        self.url = connection.url
        self.schema = connection.schema
        self.catalog = connection.catalog
        self.url_params = connection.url_params
        self.extra_configs = (
            json.dumps(connection.extra_configs)
            if connection.extra_configs is not None
            else None
        )
        self.secret_resolver_type = secret_resolver_type
        self.secret_resolver_arg = secret_resolver_arg

    def as_model_instance(self) -> M.Connection:
        secret_resolver: Optional[M.SecretResolver] = None
        if self.secret_resolver_type == "env":
            secret_resolver = M.EnvVarSecretResolver(self.secret_resolver_arg)
        elif self.secret_resolver_type == "const":
            secret_resolver = M.ConstSecretResolver(self.secret_resolver_arg)
        elif self.secret_resolver_type == "prompt":
            secret_resolver = M.PromptSecretResolver(self.secret_resolver_arg)

        return M.Connection(
            connection_name=self.connection_name,
            connection_type=M.ConnectionType(self.connection_type),
            id=self.connection_id,
            user_name=self.user_name,
            host=self.host,
            port=self.port,
            url=self.url,
            schema=self.schema,
            catalog=self.catalog,
            url_params=self.url_params,
            extra_configs=json.loads(self.extra_configs),
            secret_resolver=secret_resolver,
        )

    @classmethod
    def from_model_instance(self, model: M.Connection) -> ConnectionStorageRecord:
        secret_resolver_type = None
        secret_resolver_arg = None
        if isinstance(model.secret_resolver, M.EnvVarSecretResolver):
            secret_resolver_type = "env"
            secret_resolver_arg = model.secret_resolver.variable_name
        elif isinstance(model.secret_resolver, M.ConstSecretResolver):
            secret_resolver_type = "const"
            secret_resolver_arg = model.secret_resolver.resolve_secret()
        elif isinstance(model.secret_resolver, M.PromptSecretResolver):
            secret_resolver_type = "prompt"
            secret_resolver_arg = model.secret_resolver.title

        return ConnectionStorageRecord(
            connection_id=model.id,
            connection_name=model.connection_name,
            connection_type=model.connection_type.value,
            user_name=model.user_name,
            host=model.host,
            port=model.port,
            url=model.url,
            schema=model.schema,
            catalog=model.catalog,
            url_params=model.url_params,
            extra_configs=json.dumps(model.extra_configs)
            if model.extra_configs is not None
            else None,
            secret_resolver_type=secret_resolver_type,
            secret_resolver_arg=secret_resolver_arg,
        )


class ProjectStorageRecord(Base):
    __tablename__ = "projects"

    project_id = SA.Column(SA.String, primary_key=True)
    connection_id = SA.Column(
        SA.String, SA.ForeignKey(ConnectionStorageRecord.connection_id)
    )
    name = SA.Column(SA.String)

    event_data_tables: List[EventDataTableStorageRecord] = []
    discovery_settings_id = SA.Column(
        SA.String, SA.ForeignKey(DiscoverySettingsStorageRecord.discovery_settings_id)
    )
    webapp_settings_id = SA.Column(
        SA.String, SA.ForeignKey(WebappSettingsStorageRecord.webapp_settings_id)
    )
    description = SA.Column(SA.String, nullable=True, default=None)

    def update(self, project: M.Project):
        self.connection_id = project.connection.id
        self.name = project.project_name
        self.description = project.description
        self.event_data_tables = [
            EventDataTableStorageRecord.from_model_instance(project.id, edt)
            for edt in project.event_data_tables
        ]
        self.discovery_settings_id = project.discovery_settings.id
        self.webapp_settings_id = project.webapp_settings.id

    def as_model_instance(
        self,
        connection: M.Connection,
        event_data_tables: List[M.EventDataTable],
        discovery_settings: M.DiscoverySettings,
        webapp_settings: M.WebappSettings,
    ) -> M.Project:
        project = M.Project(
            connection=connection,
            event_data_tables=event_data_tables,
            project_name=self.name,
            description=self.description,
            discovery_settings=discovery_settings,
            webapp_settings=webapp_settings,
        )

        object.__setattr__(project, "id", self.project_id)
        return project

    @classmethod
    def from_model_instance(self, project: M.Project) -> ProjectStorageRecord:
        return ProjectStorageRecord(
            project_id=project.id,
            connection_id=project.connection.id,
            name=project.project_name,
            event_data_tables=[
                EventDataTableStorageRecord.from_model_instance(project.id, edt)
                for edt in project.event_data_tables
            ],
            discovery_settings_id=project.discovery_settings.id,
            webapp_settings_id=project.webapp_settings.id,
            description=project.description,
        )


class EventDataTableStorageRecord(Base):
    __tablename__ = "event_data_tables"

    event_data_table_id = SA.Column(SA.String, primary_key=True)
    project_id = SA.Column(
        SA.String, SA.ForeignKey(ProjectStorageRecord.project_id, ondelete="CASCADE")
    )

    table_name = SA.Column(SA.String)
    event_time_field = SA.Column(SA.String)
    user_id_field = SA.Column(SA.String)

    schema = SA.Column(SA.String, nullable=True)
    catalog = SA.Column(SA.String, nullable=True)
    event_name_field = SA.Column(SA.String, nullable=True)
    date_partition_field = SA.Column(SA.String, nullable=True)
    event_name_alias = SA.Column(SA.String, nullable=True)

    ignored_fields = SA.Column(SA.String, default="[]")
    event_specific_fields = SA.Column(SA.String, nullable=True)

    description = SA.Column(SA.String, nullable=True)

    discovery_settings_id = SA.Column(
        SA.String,
        SA.ForeignKey(DiscoverySettingsStorageRecord.discovery_settings_id),
        nullable=True,
    )

    def update(self, edt: M.EventDataTable):
        self.table_name = edt.table_name
        self.event_time_field = edt.event_time_field._get_name()
        self.user_id_field = edt.user_id_field

        self.schema = edt.schema
        self.catalog = edt.catalog
        self.event_name_field = (
            edt.event_name_field._get_name()
            if edt.event_name_field is not None
            else None
        )
        self.date_partition_field = (
            edt.date_partition_field._get_name()
            if edt.date_partition_field is not None
            else None
        )
        self.event_name_alias = edt.event_name_alias

        self.ignored_fields = json.dumps([f._get_name() for f in edt.ignored_fields])
        self.event_specific_fields = (
            json.dumps([f._get_name() for f in edt.event_specific_fields])
            if edt.event_specific_fields
            else None
        )

        self.description = edt.description

        self.discovery_settings_id = (
            edt.discovery_settings.id if edt.discovery_settings is not None else None
        )

    def as_model_instance(
        self,
        discovery_settings: Optional[M.DiscoverySettings],
    ) -> M.EventDataTable:
        edt = M.EventDataTable.create(
            self.table_name,
            self.event_time_field,
            self.user_id_field,
            schema=self.schema,
            catalog=self.catalog,
            event_name_field=self.event_name_field,
            date_partition_field=self.date_partition_field,
            event_name_alias=self.event_name_alias,
            ignored_fields=json.loads(self.ignored_fields),
            event_specific_fields=json.loads(self.event_specific_fields)
            if self.event_specific_fields
            else None,
            description=self.description,
            discovery_settings=discovery_settings,
        )

        object.__setattr__(edt, "id", self.event_data_table_id)
        return edt

    @classmethod
    def from_model_instance(
        self, project_id: str, edt: M.EventDataTable
    ) -> EventDataTableStorageRecord:
        return EventDataTableStorageRecord(
            project_id=project_id,
            event_data_table_id=edt.id,
            table_name=edt.table_name,
            event_time_field=edt.event_time_field._get_name(),
            user_id_field=edt.user_id_field._get_name(),
            schema=edt.schema,
            catalog=edt.catalog,
            event_name_field=edt.event_name_field._get_name()
            if edt.event_name_field is not None
            else None,
            date_partition_field=edt.date_partition_field._get_name()
            if edt.date_partition_field is not None
            else None,
            event_name_alias=edt.event_name_alias,
            ignored_fields=json.dumps([f._get_name() for f in edt.ignored_fields]),
            event_specific_fields=json.dumps(
                [f._get_name() for f in edt.event_specific_fields]
            )
            if edt.event_specific_fields
            else None,
            description=edt.description,
            discovery_settings_id=edt.discovery_settings.id
            if edt.discovery_settings is not None
            else None,
        )


class EventDefStorageRecord(Base):
    __tablename__ = "discovered_event_data_fields"
    id = SA.Column(SA.String, primary_key=True)
    event_data_table_id = SA.Column(
        SA.String,
        SA.ForeignKey(
            EventDataTableStorageRecord.event_data_table_id, ondelete="CASCADE"
        ),
        primary_key=True,
    )
    event_name = SA.Column(SA.String)
    fields = SA.Column(SA.String)
    description = SA.Column(SA.String, nullable=True)

    def as_model_instance(
        self,
        edt: M.EventDataTable,
    ) -> M.EventDef:
        fields_dict = json.loads(self.fields)
        fields: Dict[M.Field, M.EventFieldDef] = {}
        for field_def in fields_dict:
            fields[deserialize_field(field_def["_key"])] = M.EventFieldDef(
                _event_name=field_def["_event_name"],
                _field=deserialize_field(field_def["_field"]),
                _event_data_table=edt,
                _description=field_def["_description"],
                _enums=field_def["_enums"],
            )

        return M.EventDef(
            _id=self.id,
            _event_name=self.event_name,
            _fields=fields,
            _event_data_table=edt,
            _description=self.description,
        )

    @classmethod
    def from_model_instance(
        self, event_data_table_id: str, event_def: M.EventDef
    ) -> EventDefStorageRecord:
        fields = []
        for field_name, field_def in event_def._fields.items():
            fields.append(
                {
                    "_key": serialize_field(field_name),
                    "_event_name": field_def._event_name,
                    "_field": serialize_field(field_def._field),
                    "_event_data_table_id": field_def._event_data_table.id,
                    "_description": field_def._description,
                    "_enums": field_def._enums,
                }
            )
        return EventDefStorageRecord(
            id=event_def.get_id(),
            event_data_table_id=event_data_table_id,
            event_name=event_def._event_name,
            fields=json.dumps(fields),  # FIXME: double serialization
            description=event_def._description,
        )


class SavedMetricStorageRecord(Base):
    __tablename__ = "saved_metrics"

    saved_metric_id = SA.Column(SA.String, primary_key=True)
    project_id = SA.Column(
        SA.String,
        SA.ForeignKey(ProjectStorageRecord.project_id, ondelete="CASCADE"),
        nullable=True,
    )
    name = SA.Column(SA.String)
    description = SA.Column(SA.String)
    chart = SA.Column(SA.String)
    image_base64 = SA.Column(SA.String)
    small_image_base64 = SA.Column(SA.String)
    created_at = SA.Column(SA.DateTime)
    last_updated_at = SA.Column(SA.DateTime)
    owner = SA.Column(SA.String, nullable=True)
    metric_json = SA.Column(SA.String)

    def update(self, saved_metric: WM.SavedMetric):
        chart_dict = SavedMetricStorageRecord.__simple_chart_to_dict(saved_metric.chart)

        self.project_id = (
            saved_metric.project.id if saved_metric.project is not None else None
        )
        self.name = saved_metric.name
        self.description = saved_metric.description
        self.chart = json.dumps(chart_dict)
        self.image_base64 = saved_metric.image_base64
        self.small_image_base64 = saved_metric.small_base64
        self.created_at = saved_metric.created_at
        self.last_updated_at = saved_metric.last_updated_at
        self.owner = saved_metric.owner
        self.metric_json = saved_metric.metric_json

    def as_model_instance(self, project: M.Project) -> WM.SavedMetric:
        return WM.SavedMetric(
            name=self.name,
            id=self.saved_metric_id,
            chart=self.__simple_chart_from_dict(json.loads(self.chart)),
            image_base64=self.image_base64,
            small_base64=self.small_image_base64,
            project=project,
            metric_json=self.metric_json,
            owner=self.owner,
            description=self.description,
            created_at=self.created_at,
            last_updated_at=self.last_updated_at,
        )

    def __simple_chart_from_dict(self, data: Dict[str, Any]) -> VC.SimpleChart:
        return VC.SimpleChart(
            title=data["title"],
            x_axis_label=data["x_axis_label"],
            y_axis_label=data["y_axis_label"],
            color_label=data["color_label"],
            yaxis_ticksuffix=data["yaxis_ticksuffix"],
            hover_mode=data["hover_mode"],
            chart_type=M.SimpleChartType(data["chart_type"]),
            dataframe=pickle.loads(base64.b64decode(data["dataframe"])),
        )

    @classmethod
    def __simple_chart_to_dict(self, chart: VC.SimpleChart) -> Dict[str, Any]:
        f = io.BytesIO()
        pickle.dump(chart.dataframe, f)
        f.seek(0)

        return {
            "title": chart.title,
            "x_axis_label": chart.x_axis_label,
            "y_axis_label": chart.y_axis_label,
            "color_label": chart.color_label,
            "yaxis_ticksuffix": chart.yaxis_ticksuffix,
            "hover_mode": chart.hover_mode,
            "chart_type": chart.chart_type.value,
            "dataframe": base64.b64encode(f.read()).decode(),
            # FIXME: label funcs are not serialized
        }

    @classmethod
    def from_model_instance(
        self, saved_metric: WM.SavedMetric
    ) -> SavedMetricStorageRecord:
        chart_dict = SavedMetricStorageRecord.__simple_chart_to_dict(saved_metric.chart)

        return SavedMetricStorageRecord(
            saved_metric_id=saved_metric.id,
            project_id=saved_metric.project.id
            if saved_metric.project is not None
            else None,
            name=saved_metric.name,
            description=saved_metric.description,
            chart=json.dumps(chart_dict),
            image_base64=saved_metric.image_base64,
            small_image_base64=saved_metric.small_base64,
            created_at=saved_metric.created_at,
            last_updated_at=saved_metric.last_updated_at,
            owner=saved_metric.owner,
            metric_json=saved_metric.metric_json,
        )


class DashboardStorageRecord(Base):
    __tablename__ = "dashboards"

    dashboard_id = SA.Column(SA.String, primary_key=True)
    name: str = SA.Column(SA.String)
    created_at = SA.Column(SA.DateTime)
    last_updated_at = SA.Column(SA.DateTime)
    owner = SA.Column(SA.String, nullable=True)

    def update(self, dashboard: WM.Dashboard):
        self.name = dashboard.name
        self.created_at = dashboard.created_at
        self.last_updated_at = dashboard.last_updated_at
        self.owner = dashboard.owner

    def as_model_instance(
        self, dashboard_metrics: List[WM.DashboardMetric]
    ) -> WM.Dashboard:
        return WM.Dashboard(
            name=self.name,
            id=self.dashboard_id,
            dashboard_metrics=dashboard_metrics,
            created_at=self.created_at,
            last_updated_at=self.last_updated_at,
            owner=self.owner,
        )

    @classmethod
    def from_model_instance(self, dashboard: WM.Dashboard) -> DashboardStorageRecord:
        return DashboardStorageRecord(
            dashboard_id=dashboard.id,
            name=dashboard.name,
            created_at=dashboard.created_at,
            last_updated_at=dashboard.last_updated_at,
            owner=dashboard.owner,
        )


class DashboardMetricStorageRecord(Base):
    __tablename__ = "dashboard_metrics"

    dashboard_metric_id = SA.Column(SA.String, primary_key=True)
    dashboard_id = SA.Column(
        SA.String,
        SA.ForeignKey(DashboardStorageRecord.dashboard_id, ondelete="CASCADE"),
    )
    saved_metric_id = SA.Column(
        SA.String,
        SA.ForeignKey(SavedMetricStorageRecord.saved_metric_id, ondelete="CASCADE"),
    )

    x = SA.Column(SA.Integer)
    y = SA.Column(SA.Integer)
    width = SA.Column(SA.Integer)
    height = SA.Column(SA.Integer)

    def as_model_instance(self, saved_metric: WM.SavedMetric) -> WM.DashboardMetric:
        return WM.DashboardMetric(
            id=self.dashboard_metric_id,
            saved_metric=saved_metric,
            x=self.x,
            y=self.y,
            width=self.width,
            height=self.height,
        )

    @classmethod
    def from_model_instance(
        self, dashboard_id, dashboard_metric: WM.DashboardMetric
    ) -> DashboardMetricStorageRecord:
        return DashboardMetricStorageRecord(
            dashboard_metric_id=dashboard_metric.id,
            dashboard_id=dashboard_id,
            saved_metric_id=dashboard_metric.get_saved_metric_id(),
            x=dashboard_metric.x,
            y=dashboard_metric.y,
            width=dashboard_metric.width,
            height=dashboard_metric.height,
        )
