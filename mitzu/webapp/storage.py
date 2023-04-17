from __future__ import annotations

from typing import Dict, List, Optional


import mitzu.model as M
import mitzu.webapp.model as WM
import mitzu.webapp.storage_model as SM
from mitzu.samples.data_ingestion import create_and_ingest_sample_project
import sqlalchemy as SA
from sqlalchemy.orm import Session

import flask

SAMPLE_PROJECT_NAME = "sample_project"
SAMPLE_PROJECT_ID = "sample_project_id"
SAMPLE_CONNECTION_ID = "sample_connection_id"


DEFAULT_CONNECTION_STRING = "sqlite://?check_same_thread=False"


class InvalidStorageReference(Exception):
    pass


class StorageEventDefReference(M.Reference[M.EventDef]):
    def __init__(
        self,
        id: Optional[str],
        value: Optional[M.EventDef],
        event_data_table: M.EventDataTable,
    ):
        super().__init__(id=id, value=value)
        self.event_data_table = event_data_table

    def get_value(self) -> Optional[M.EventDef]:
        res = self._value_state.get_value()
        if res is None:
            deps = flask.current_app.config.get("dependencies")
            if deps is None:
                raise InvalidStorageReference("No storage in request dependencies")
            res = deps.storage.get_event_definition(
                event_definition_id=self._id, event_data_table=self.event_data_table
            )
            self.restore_value(res)
        return res


def setup_sample_project(storage: MitzuStorage):
    if storage.project_exists(SAMPLE_PROJECT_ID):
        return
    connection = M.Connection(
        id=SAMPLE_CONNECTION_ID,
        connection_name="Sample connection",
        connection_type=M.ConnectionType.SQLITE,
        host="sample_project",
    )
    project = create_and_ingest_sample_project(
        connection,
        event_count=200000,
        number_of_users=300,
        schema="main",
        overwrite_records=False,
        project_id=SAMPLE_PROJECT_ID,
    )
    storage.set_connection(project.connection.id, project.connection)
    storage.set_project(project_id=project.id, project=project)

    dp = project.discover_project()

    for edt, defs in dp.definitions.items():
        storage.set_event_data_table_definition(event_data_table=edt, definitions=defs)


class MitzuStorage:
    def __init__(
        self,
        connection_string: str = "sqlite://?check_same_thread=False",
    ) -> None:
        self._engine = SA.create_engine(connection_string)
        if connection_string.startswith("sqlite"):
            with self._engine.connect() as connection:
                connection.execute("PRAGMA foreign_keys = ON;")
        self.__init_schema()

    @property
    def _session(self) -> Session:
        if flask.has_app_context():
            if "request_session_cache" not in flask.g:
                session = SA.orm.sessionmaker(bind=self._engine)()
                session.execute("PRAGMA foreign_keys = ON;")
                session.commit()
                flask.g.request_session_cache = session
                return session
            else:
                return flask.g.request_session_cache
        else:
            session = SA.orm.sessionmaker(bind=self._engine)()
            session.execute("PRAGMA foreign_keys = ON;")
            session.commit()
            return session

    def __init_schema(self):
        SM.Base.metadata.create_all(self._engine)

    def set_project(self, project_id: str, project: M.Project):
        self.set_connection(project.connection.id, project.connection)
        self.set_discovery_settings(
            project.discovery_settings.id, project.discovery_settings
        )
        self.set_webapp_settings(project.webapp_settings.id, project.webapp_settings)

        current_edt_ids = [edt.id for edt in project.event_data_tables]
        self._remove_unreferenced_event_data_tables(project.id, current_edt_ids)

        record = (
            self._session.query(SM.ProjectStorageRecord)
            .filter(SM.ProjectStorageRecord.project_id == project_id)
            .first()
        )

        if record is None:
            self._session.add(SM.ProjectStorageRecord.from_model_instance(project))
        else:
            record.update(project)

        for edt in project.event_data_tables:
            self._set_event_data_table(project.id, edt.id, edt)

        discovered_project = project._discovered_project.get_value()
        if discovered_project:
            self.populate_discovered_project(discovered_project)
            for edt, vals in discovered_project.definitions.items():
                self.set_event_data_table_definition(edt, vals)

        self._session.commit()

    def project_exists(self, project_id: str) -> bool:
        return (
            self._session.query(SM.ProjectStorageRecord)
            .filter(SM.ProjectStorageRecord.project_id == project_id)
            .first()
        ) is not None

    def get_project(self, project_id: str) -> M.Project:
        record = (
            self._session.query(SM.ProjectStorageRecord)
            .filter(SM.ProjectStorageRecord.project_id == project_id)
            .first()
        )
        if record is None:
            raise ValueError(f"Project not found with project_id={project_id}")

        connection = self.get_connection(record.connection_id)
        discovery_settings = self.get_discovery_settings(record.discovery_settings_id)
        webapp_settings = self.get_webapp_settings(record.webapp_settings_id)
        edts = self._get_event_data_tables_for_project(record.project_id)
        project = record.as_model_instance(
            connection, edts, discovery_settings, webapp_settings
        )

        discovered_definitions: Dict[
            M.EventDataTable, Dict[str, M.Reference[M.EventDef]]
        ] = {}
        for edt in edts:
            discovered_fields = (
                self._session.query(
                    SM.EventDefStorageRecord.event_name, SM.EventDefStorageRecord.id
                )
                .filter(SM.EventDefStorageRecord.event_data_table_id == edt.id)
                .all()
            )
            definitions: Dict[str, M.Reference[M.EventDef]] = {}
            for field in discovered_fields:
                definitions[field.event_name] = StorageEventDefReference(
                    id=field.id, value=None, event_data_table=edt
                )
            if len(definitions) > 0:
                discovered_definitions[edt] = definitions

        if len(discovered_definitions) > 0:
            # it may seems a bit od but the constructor will put the reference of the discovered project into the project
            M.DiscoveredProject(discovered_definitions, project)
        return project

    def delete_project(self, project_id: str):
        session = self._session
        record = (
            session.query(SM.ProjectStorageRecord)
            .filter(SM.ProjectStorageRecord.project_id == project_id)
            .first()
        )
        if record is not None:
            session.delete(record)
            session.commit()

    def _set_event_data_table(
        self, project_id: str, edt_id: str, edt: M.EventDataTable
    ):
        if edt.discovery_settings:
            self.set_discovery_settings(
                edt.discovery_settings.id, edt.discovery_settings
            )

        record = (
            self._session.query(SM.EventDataTableStorageRecord)
            .filter(SM.EventDataTableStorageRecord.event_data_table_id == edt_id)
            .first()
        )
        if record is None:
            self._session.add(
                SM.EventDataTableStorageRecord.from_model_instance(project_id, edt)
            )
            return

        record.update(edt)

        self._session.commit()

    def _get_event_data_tables_for_project(
        self, project_id: str
    ) -> List[M.EventDataTable]:
        records = self._session.query(SM.EventDataTableStorageRecord).filter(
            SM.EventDataTableStorageRecord.project_id == project_id
        )
        result = []
        for edt_record in records.all():
            discovery_settings = self.get_discovery_settings(
                edt_record.discovery_settings_id
            )
            result.append(edt_record.as_model_instance(discovery_settings))
        return result

    def _remove_unreferenced_event_data_tables(
        self, project_id: str, referenced_edts: List[str]
    ):
        records = (
            self._session.query(SM.EventDataTableStorageRecord)
            .filter(
                SM.EventDataTableStorageRecord.project_id == project_id
                and SM.EventDataTableStorageRecord.event_data_table_id
                not in referenced_edts
            )
            .all()
        )
        for record in records:
            self._session.delete(record)

    def set_discovery_settings(
        self, discovery_settings_id: str, discovery_settings: M.DiscoverySettings
    ):
        record = (
            self._session.query(SM.DiscoverySettingsStorageRecord)
            .filter(
                SM.DiscoverySettingsStorageRecord.discovery_settings_id
                == discovery_settings_id
            )
            .first()
        )
        if record is None:
            self._session.add(
                SM.DiscoverySettingsStorageRecord.from_model_instance(
                    discovery_settings
                )
            )
            self._session.commit()
            return

        record.update(discovery_settings)

        self._session.commit()

    def get_discovery_settings(self, discovery_settings_id: str) -> M.DiscoverySettings:
        record = (
            self._session.query(SM.DiscoverySettingsStorageRecord)
            .filter(
                SM.DiscoverySettingsStorageRecord.discovery_settings_id
                == discovery_settings_id
            )
            .first()
        )

        if record is None:
            raise ValueError(
                f"Discovery settings not found with project_id={discovery_settings_id}"
            )

        return record.as_model_instance()

    def set_webapp_settings(
        self, webapp_settings_id: str, webapp_settings: M.WebappSettings
    ):
        record = (
            self._session.query(SM.WebappSettingsStorageRecord)
            .filter(
                SM.WebappSettingsStorageRecord.webapp_settings_id == webapp_settings_id
            )
            .first()
        )
        if record is None:
            self._session.add(
                SM.WebappSettingsStorageRecord.from_model_instance(webapp_settings)
            )
            return

        record.update(webapp_settings)

        self._session.commit()

    def get_webapp_settings(self, webapp_settings_id: str) -> M.WebappSettings:
        record = (
            self._session.query(SM.WebappSettingsStorageRecord)
            .filter(
                SM.WebappSettingsStorageRecord.webapp_settings_id == webapp_settings_id
            )
            .first()
        )

        if record is None:
            raise ValueError(
                f"Webapp settings not found with project_id={webapp_settings_id}"
            )

        return record.as_model_instance()

    def list_projects(self) -> List[str]:
        result = []
        for record in self._session.query(SM.ProjectStorageRecord):
            result.append(record.project_id)
        return result

    def set_connection(self, connection_id: str, connection: M.Connection):
        session = self._session
        record = (
            session.query(SM.ConnectionStorageRecord)
            .filter(SM.ConnectionStorageRecord.connection_id == connection_id)
            .first()
        )
        if record is None:
            session.add(SM.ConnectionStorageRecord.from_model_instance(connection))
        else:
            record.update(connection)

        session.commit()

    def get_connection(self, connection_id: str) -> M.Connection:
        record = (
            self._session.query(SM.ConnectionStorageRecord)
            .filter(SM.ConnectionStorageRecord.connection_id == connection_id)
            .first()
        )

        if record is None:
            raise ValueError(f"Connection not found with project_id={connection_id}")

        return record.as_model_instance()

    def delete_connection(self, connection_id: str):
        session = self._session
        record = (
            session.query(SM.ConnectionStorageRecord)
            .filter(SM.ConnectionStorageRecord.connection_id == connection_id)
            .first()
        )
        if record is not None:
            session.delete(record)
            session.commit()

    def list_connections(
        self,
    ) -> List[str]:
        result = []
        for record in self._session.query(SM.ConnectionStorageRecord):
            result.append(record.connection_id)
        return result

    def set_event_data_table_definition(
        self,
        event_data_table: M.EventDataTable,
        definitions: Dict[str, M.Reference[M.EventDef]],
    ):
        edt_id = event_data_table.id
        for event_name, event_def in definitions.items():
            rec = (
                self._session.query(SM.EventDefStorageRecord)
                .filter(
                    SM.EventDefStorageRecord.event_data_table_id == edt_id
                    and SM.EventDefStorageRecord.event_name == event_name
                )
                .first()
            )
            if rec is not None:
                self._session.delete(rec)

            rec = SM.EventDefStorageRecord.from_model_instance(
                edt_id, event_def.get_value_if_exists()
            )
            self._session.add(rec)

        self._session.commit()

    def populate_discovered_project(self, discovered_project: M.DiscoveredProject):
        defs = discovered_project.definitions

        for edt in defs.keys():
            records: List[SM.EventDefStorageRecord] = (
                self._session.query(SM.EventDefStorageRecord)
                .filter(SM.EventDefStorageRecord.event_data_table_id == edt.id)
                .all()
            )

            for rec in records:
                defs[edt][rec.event_name].restore_value(rec.as_model_instance(edt))

    def get_event_definition(
        self, event_data_table: M.EventDataTable, event_definition_id: str
    ) -> M.EventDef:
        record: SM.EventDefStorageRecord = (
            self._session.query(SM.EventDefStorageRecord)
            .filter(SM.EventDefStorageRecord.id == event_definition_id)
            .first()
        )
        return record.as_model_instance(edt=event_data_table)

    def set_saved_metric(self, metric_id: str, saved_metric: WM.SavedMetric):
        record = (
            self._session.query(SM.SavedMetricStorageRecord)
            .filter(SM.SavedMetricStorageRecord.saved_metric_id == metric_id)
            .first()
        )
        if record is None:
            self._session.add(
                SM.SavedMetricStorageRecord.from_model_instance(saved_metric)
            )
            self._session.commit()
            return

        record.update(saved_metric)

        self._session.commit()

    def get_saved_metric(self, metric_id: str) -> WM.SavedMetric:
        record = (
            self._session.query(SM.SavedMetricStorageRecord)
            .filter(SM.SavedMetricStorageRecord.saved_metric_id == metric_id)
            .first()
        )
        if record is None:
            raise ValueError(f"Saved metric is not found with id {metric_id}")

        project = self.get_project(record.project_id)
        return record.as_model_instance(project)

    def clear_saved_metric(self, metric_id: str):
        record = (
            self._session.query(SM.SavedMetricStorageRecord)
            .filter(SM.SavedMetricStorageRecord.saved_metric_id == metric_id)
            .first()
        )
        if record is not None:
            self._session.delete(record)
            self._session.commit()

    def list_saved_metrics(self) -> List[str]:
        result = []
        for record in self._session.query(SM.SavedMetricStorageRecord):
            result.append(record.saved_metric_id)
        return result

    def list_dashboards(self) -> List[str]:
        result = []
        for record in self._session.query(SM.DashboardStorageRecord):
            result.append(record.dashboard_id)
        return result

    def get_dashboard(self, dashboard_id: str) -> WM.Dashboard:
        record = (
            self._session.query(SM.DashboardStorageRecord)
            .filter(SM.DashboardStorageRecord.dashboard_id == dashboard_id)
            .first()
        )
        if record is None:
            raise ValueError(f"Dashboard is not found with id {dashboard_id}")

        dashboard_metric_records = (
            self._session.query(SM.DashboardMetricStorageRecord)
            .filter(SM.DashboardMetricStorageRecord.dashboard_id == dashboard_id)
            .all()
        )
        dashboard_metrics = []
        for dm in dashboard_metric_records:
            sm = self.get_saved_metric(dm.saved_metric_id)
            dashboard_metrics.append(dm.as_model_instance(sm))

        return record.as_model_instance(dashboard_metrics)

    def set_dashboard(self, dashboard_id: str, dashboard: WM.Dashboard):
        record = (
            self._session.query(SM.DashboardStorageRecord)
            .filter(SM.DashboardStorageRecord.dashboard_id == dashboard_id)
            .first()
        )
        if record is None:
            self._session.add(SM.DashboardStorageRecord.from_model_instance(dashboard))
        else:
            record.update(dashboard)

        dashboard_metrics = (
            self._session.query(SM.DashboardMetricStorageRecord)
            .filter(SM.DashboardMetricStorageRecord.dashboard_id == dashboard.id)
            .all()
        )
        for dm in dashboard_metrics:
            self._session.delete(dm)

        self._set_dashboard_metrics(dashboard.id, dashboard.dashboard_metrics)
        self._session.commit()

    def _set_dashboard_metrics(
        self, dashboard_id: str, metrics: List[WM.DashboardMetric]
    ):
        for dashboard_metric in metrics:
            self._session.add(
                SM.DashboardMetricStorageRecord.from_model_instance(
                    dashboard_id, dashboard_metric
                )
            )

    def clear_dashboard(self, dashboard_id: str):
        record = (
            self._session.query(SM.DashboardStorageRecord)
            .filter(SM.DashboardStorageRecord.dashboard_id == dashboard_id)
            .first()
        )
        if record is not None:
            self._session.delete(record)
            self._session.commit()

    def set_user(self, user: WM.User):
        session = self._session
        record = (
            session.query(SM.UserStorageRecord)
            .filter(SM.UserStorageRecord.user_id == user.id)
            .first()
        )
        if record is None:
            session.add(SM.UserStorageRecord.from_model_instance(user))
            session.commit()
            return

        record.update(user)
        session.commit()

    def get_user_by_id(self, user_id: str) -> Optional[WM.User]:
        record = (
            self._session.query(SM.UserStorageRecord)
            .filter(SM.UserStorageRecord.user_id == user_id)
            .first()
        )
        if record is None:
            return None

        return record.as_model_instance()

    def list_users(self) -> List[WM.User]:
        result = []
        for record in self._session.query(SM.UserStorageRecord):
            result.append(record.as_model_instance())
        return result

    def clear_user(self, user_id: str):
        session = self._session
        record = (
            session.query(SM.UserStorageRecord)
            .filter(SM.UserStorageRecord.user_id == user_id)
            .first()
        )
        if record is not None:
            session.delete(record)
            session.commit()
