from __future__ import annotations

import multiprocessing
from typing import Dict, List, Optional

from mitzu.helper import LOGGER
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
        self.__pid = None
        self.__is_sqlite = connection_string.startswith("sqlite")
        self.__connection_string = connection_string

    def _new_db_session(self) -> Session:
        self.__create_engine_when_needed()
        session = SA.orm.sessionmaker(bind=self._engine)()
        if self.__is_sqlite:
            session.execute("PRAGMA foreign_keys = ON;")
            session.commit()
        return session

    def __create_engine_when_needed(self):
        # we need to make sure that the engine is created by the current process and not by the parent process
        pid = multiprocessing.current_process().pid
        if self.__pid != pid:
            LOGGER.debug(
                f"Engine needs to be recreated, previous instance created by pid: {self.__pid}, current pid: {pid}"
            )
            self._engine = SA.create_engine(
                self.__connection_string, pool_pre_ping=True
            )
            self.__pid = pid

    def init_db_schema(self):
        LOGGER.debug("Initializing the database schema")
        self.__create_engine_when_needed()
        tables = []
        for storage_record in [
            SM.UserStorageRecord,
            SM.DiscoverySettingsStorageRecord,
            SM.WebappSettingsStorageRecord,
            SM.ConnectionStorageRecord,
            SM.ProjectStorageRecord,
            SM.EventDataTableStorageRecord,
            SM.EventDefStorageRecord,
            SM.SavedMetricStorageRecord,
            SM.DashboardStorageRecord,
            SM.DashboardMetricStorageRecord,
        ]:
            tables.append(SM.Base.metadata.tables[storage_record.__tablename__])

        SM.Base.metadata.create_all(self._engine, tables=tables)

    def set_project(self, project_id: str, project: M.Project):
        with self._new_db_session() as session:
            self._set_connection(project.connection.id, project.connection, session)
            self._set_discovery_settings(
                project.discovery_settings.id, project.discovery_settings, session
            )
            self._set_webapp_settings(
                project.webapp_settings.id, project.webapp_settings, session
            )

            current_edt_ids = [edt.id for edt in project.event_data_tables]
            self._remove_unreferenced_event_data_tables(
                project.id, current_edt_ids, session
            )

            record = (
                session.query(SM.ProjectStorageRecord)
                .filter(SM.ProjectStorageRecord.project_id == project_id)
                .first()
            )

            if record is None:
                session.add(SM.ProjectStorageRecord.from_model_instance(project))
            else:
                record.update(project)

            for edt in project.event_data_tables:
                self._set_event_data_table(project.id, edt, session)

            discovered_project = project._discovered_project.get_value()
            if discovered_project:
                self._populate_discovered_project(discovered_project, session)
                for edt, vals in discovered_project.definitions.items():
                    self._set_event_data_table_definition(edt, vals, session)

            session.commit()

    def project_exists(self, project_id: str) -> bool:
        with self._new_db_session() as session:
            return (
                session.query(SM.ProjectStorageRecord)
                .filter(SM.ProjectStorageRecord.project_id == project_id)
                .first()
            ) is not None

    def get_project(self, project_id: str) -> M.Project:
        with self._new_db_session() as session:
            record = (
                session.query(SM.ProjectStorageRecord)
                .filter(SM.ProjectStorageRecord.project_id == project_id)
                .first()
            )
            if record is None:
                raise ValueError(f"Project not found with project_id={project_id}")

            connection = self._get_connection(record.connection_id, session)
            discovery_settings = self._get_discovery_settings(
                record.discovery_settings_id, session
            )
            webapp_settings = self._get_webapp_settings(
                record.webapp_settings_id, session
            )
            edts = self._get_event_data_tables_for_project(record.project_id, session)
            project = record.as_model_instance(
                connection, edts, discovery_settings, webapp_settings
            )

            discovered_definitions: Dict[
                M.EventDataTable, Dict[str, M.Reference[M.EventDef]]
            ] = {}
            for edt in edts:
                discovered_fields = (
                    session.query(
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
        with self._new_db_session() as session:
            record = (
                session.query(SM.ProjectStorageRecord)
                .filter(SM.ProjectStorageRecord.project_id == project_id)
                .first()
            )
            if record is not None:
                session.delete(record)
                session.commit()

    def _set_event_data_table(
        self, project_id: str, edt: M.EventDataTable, session: SA.orm.Session
    ):
        if edt.discovery_settings:
            self._set_discovery_settings(
                edt.discovery_settings.id, edt.discovery_settings, session
            )
        record = (
            session.query(SM.EventDataTableStorageRecord)
            .filter(
                (SM.EventDataTableStorageRecord.project_id == project_id)
                & (SM.EventDataTableStorageRecord.table_name == edt.table_name)
            )
            .first()
        )
        if record is not None:
            session.delete(record)

        rec = SM.EventDataTableStorageRecord.from_model_instance(
            project_id=project_id, edt=edt
        )
        session.add(rec)

    def _get_event_data_tables_for_project(
        self, project_id: str, session: SA.orm.Session
    ) -> List[M.EventDataTable]:
        records = session.query(SM.EventDataTableStorageRecord).filter(
            SM.EventDataTableStorageRecord.project_id == project_id
        )
        result = []
        for edt_record in records.all():
            discovery_settings = self._get_discovery_settings(
                edt_record.discovery_settings_id, session
            )
            result.append(edt_record.as_model_instance(discovery_settings))
        return result

    def _remove_unreferenced_event_data_tables(
        self, project_id: str, referenced_edts: List[str], session: SA.orm.Session
    ):
        records = (
            session.query(SM.EventDataTableStorageRecord)
            .filter(
                (SM.EventDataTableStorageRecord.project_id == project_id)
                & (
                    SM.EventDataTableStorageRecord.event_data_table_id
                    not in referenced_edts
                )
            )
            .all()
        )
        for record in records:
            session.delete(record)

    def _set_discovery_settings(
        self,
        discovery_settings_id: str,
        discovery_settings: M.DiscoverySettings,
        session: SA.orm.Session,
    ):
        record = (
            session.query(SM.DiscoverySettingsStorageRecord)
            .filter(
                SM.DiscoverySettingsStorageRecord.discovery_settings_id
                == discovery_settings_id
            )
            .first()
        )
        if record is None:
            session.add(
                SM.DiscoverySettingsStorageRecord.from_model_instance(
                    discovery_settings
                )
            )
            session.commit()
            return

        record.update(discovery_settings)

    def _get_discovery_settings(
        self, discovery_settings_id: str, session: SA.orm.Session
    ) -> M.DiscoverySettings:
        record = (
            session.query(SM.DiscoverySettingsStorageRecord)
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

    def _set_webapp_settings(
        self,
        webapp_settings_id: str,
        webapp_settings: M.WebappSettings,
        session: SA.orm.Session,
    ):
        record = (
            session.query(SM.WebappSettingsStorageRecord)
            .filter(
                SM.WebappSettingsStorageRecord.webapp_settings_id == webapp_settings_id
            )
            .first()
        )
        if record is None:
            session.add(
                SM.WebappSettingsStorageRecord.from_model_instance(webapp_settings)
            )
            return

        record.update(webapp_settings)

    def _get_webapp_settings(
        self, webapp_settings_id: str, session: SA.orm.Session
    ) -> M.WebappSettings:
        record = (
            session.query(SM.WebappSettingsStorageRecord)
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
        with self._new_db_session() as session:
            for record in session.query(SM.ProjectStorageRecord):
                result.append(record.project_id)
            return result

    def set_connection(self, connection_id: str, connection: M.Connection):
        with self._new_db_session() as session:
            self._set_connection(connection_id, connection, session)
            session.commit()

    def _set_connection(
        self, connection_id: str, connection: M.Connection, session: SA.orm.Session
    ):
        record = (
            session.query(SM.ConnectionStorageRecord)
            .filter(SM.ConnectionStorageRecord.connection_id == connection_id)
            .first()
        )
        if record is None:
            session.add(SM.ConnectionStorageRecord.from_model_instance(connection))
        else:
            record.update(connection)

    def get_connection(self, connection_id: str) -> M.Connection:
        with self._new_db_session() as session:
            return self._get_connection(connection_id, session)

    def _get_connection(
        self, connection_id: str, session: SA.orm.Session
    ) -> M.Connection:
        record = (
            session.query(SM.ConnectionStorageRecord)
            .filter(SM.ConnectionStorageRecord.connection_id == connection_id)
            .first()
        )

        if record is None:
            raise ValueError(f"Connection not found with project_id={connection_id}")

        return record.as_model_instance()

    def delete_connection(self, connection_id: str):
        with self._new_db_session() as session:
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
        with self._new_db_session() as session:
            result = []
            for record in session.query(SM.ConnectionStorageRecord):
                result.append(record.connection_id)
            return result

    def set_event_data_table_definition(
        self,
        event_data_table: M.EventDataTable,
        definitions: Dict[str, M.Reference[M.EventDef]],
    ):
        with self._new_db_session() as session:
            self._set_event_data_table_definition(
                event_data_table, definitions, session
            )
            session.commit()

    def _set_event_data_table_definition(
        self,
        event_data_table: M.EventDataTable,
        definitions: Dict[str, M.Reference[M.EventDef]],
        session: SA.orm.Session,
    ):
        edt_id = event_data_table.id
        for event_name, event_def in definitions.items():
            rec = (
                session.query(SM.EventDefStorageRecord)
                .filter(
                    (SM.EventDefStorageRecord.event_data_table_id == edt_id)
                    & (SM.EventDefStorageRecord.event_name == event_name)
                )
                .first()
            )
            if rec is not None:
                session.delete(rec)

            rec = SM.EventDefStorageRecord.from_model_instance(
                edt_id, event_def.get_value_if_exists()
            )
            session.add(rec)

    def populate_discovered_project(self, discovered_project: M.DiscoveredProject):
        with self._new_db_session() as session:
            self._populate_discovered_project(discovered_project, session)

    def _populate_discovered_project(
        self, discovered_project: M.DiscoveredProject, session: SA.orm.Session
    ):
        defs = discovered_project.definitions

        for edt in defs.keys():
            records: List[SM.EventDefStorageRecord] = (
                session.query(SM.EventDefStorageRecord)
                .filter(SM.EventDefStorageRecord.event_data_table_id == edt.id)
                .all()
            )

            for rec in records:
                defs[edt][rec.event_name].restore_value(rec.as_model_instance(edt))

    def get_event_definition(
        self, event_data_table: M.EventDataTable, event_definition_id: str
    ) -> M.EventDef:
        with self._new_db_session() as session:
            record: SM.EventDefStorageRecord = (
                session.query(SM.EventDefStorageRecord)
                .filter(SM.EventDefStorageRecord.id == event_definition_id)
                .first()
            )
            return record.as_model_instance(edt=event_data_table)

    def set_saved_metric(self, metric_id: str, saved_metric: WM.SavedMetric):
        with self._new_db_session() as session:
            record = (
                session.query(SM.SavedMetricStorageRecord)
                .filter(SM.SavedMetricStorageRecord.saved_metric_id == metric_id)
                .first()
            )
            if record is None:
                session.add(
                    SM.SavedMetricStorageRecord.from_model_instance(saved_metric)
                )
                session.commit()
                return

            record.update(saved_metric)

            session.commit()

    def get_saved_metric(self, metric_id: str) -> WM.SavedMetric:
        with self._new_db_session() as session:
            record = (
                session.query(SM.SavedMetricStorageRecord)
                .filter(SM.SavedMetricStorageRecord.saved_metric_id == metric_id)
                .first()
            )
            if record is None:
                raise ValueError(f"Saved metric is not found with id {metric_id}")

        project = self.get_project(record.project_id)
        return record.as_model_instance(project)

    def clear_saved_metric(self, metric_id: str):
        with self._new_db_session() as session:
            record = (
                session.query(SM.SavedMetricStorageRecord)
                .filter(SM.SavedMetricStorageRecord.saved_metric_id == metric_id)
                .first()
            )
            if record is not None:
                session.delete(record)
                session.commit()

    def list_saved_metrics(self) -> List[str]:
        with self._new_db_session() as session:
            result = []
            for record in session.query(SM.SavedMetricStorageRecord):
                result.append(record.saved_metric_id)
            return result

    def list_dashboards(self) -> List[str]:
        result = []
        with self._new_db_session() as session:
            for record in session.query(SM.DashboardStorageRecord):
                result.append(record.dashboard_id)
            return result

    def get_dashboard(self, dashboard_id: str) -> WM.Dashboard:
        with self._new_db_session() as session:
            record = (
                session.query(SM.DashboardStorageRecord)
                .filter(SM.DashboardStorageRecord.dashboard_id == dashboard_id)
                .first()
            )
            if record is None:
                raise ValueError(f"Dashboard is not found with id {dashboard_id}")

            dashboard_metric_records = (
                session.query(SM.DashboardMetricStorageRecord)
                .filter(SM.DashboardMetricStorageRecord.dashboard_id == dashboard_id)
                .all()
            )
            dashboard_metrics = []
            for dm in dashboard_metric_records:
                sm = self.get_saved_metric(dm.saved_metric_id)
                dashboard_metrics.append(dm.as_model_instance(sm))

            return record.as_model_instance(dashboard_metrics)

    def set_dashboard(self, dashboard_id: str, dashboard: WM.Dashboard):
        with self._new_db_session() as session:
            record = (
                session.query(SM.DashboardStorageRecord)
                .filter(SM.DashboardStorageRecord.dashboard_id == dashboard_id)
                .first()
            )
            if record is None:
                session.add(SM.DashboardStorageRecord.from_model_instance(dashboard))
            else:
                record.update(dashboard)

            dashboard_metrics = (
                session.query(SM.DashboardMetricStorageRecord)
                .filter(SM.DashboardMetricStorageRecord.dashboard_id == dashboard.id)
                .all()
            )
            for dm in dashboard_metrics:
                session.delete(dm)

            self._set_dashboard_metrics(
                dashboard.id, dashboard.dashboard_metrics, session
            )
            session.commit()

    def _set_dashboard_metrics(
        self,
        dashboard_id: str,
        metrics: List[WM.DashboardMetric],
        session: SA.orm.Session,
    ):
        for dashboard_metric in metrics:
            session.add(
                SM.DashboardMetricStorageRecord.from_model_instance(
                    dashboard_id, dashboard_metric
                )
            )

    def clear_dashboard(self, dashboard_id: str):
        with self._new_db_session() as session:
            record = (
                session.query(SM.DashboardStorageRecord)
                .filter(SM.DashboardStorageRecord.dashboard_id == dashboard_id)
                .first()
            )
            if record is not None:
                session.delete(record)
                session.commit()

    def set_user(self, user: WM.User):
        with self._new_db_session() as session:
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
        with self._new_db_session() as session:
            record = (
                session.query(SM.UserStorageRecord)
                .filter(SM.UserStorageRecord.user_id == user_id)
                .first()
            )
            if record is None:
                return None

            return record.as_model_instance()

    def list_users(self) -> List[WM.User]:
        result = []
        with self._new_db_session() as session:
            for record in session.query(SM.UserStorageRecord):
                result.append(record.as_model_instance())
            return result

    def clear_user(self, user_id: str):
        with self._new_db_session() as session:
            record = (
                session.query(SM.UserStorageRecord)
                .filter(SM.UserStorageRecord.user_id == user_id)
                .first()
            )
            if record is not None:
                session.delete(record)
                session.commit()

    def health_check(self):
        with self._new_db_session() as session:
            session.execute("select 1")
