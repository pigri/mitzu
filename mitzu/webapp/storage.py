from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

import mitzu.model as M
import mitzu.webapp.cache as C
import mitzu.helper as H
import mitzu.webapp.model as WM
from mitzu.samples.data_ingestion import create_and_ingest_sample_project

SAMPLE_PROJECT_NAME = "sample_project"
SAMPLE_PROJECT_ID = "sample_project_id"
SAMPLE_CONNECTION_ID = "sample_connection_id"


DEFAULT_EXPIRE_TIME = 600  # 10 minutes

PROJECT_PREFIX = "__project__"
CONNECTION_PREFIX = "__conn__"
SEPC_PROJECT_PREFIX = "__spec_project__"
TABLE_DEFIONITION_PREFIX = "__table_definition__"
TABLE_PREFIX = "__table__"
SIMPLE_CHART_PREFIX = "__simple_chart__"
SAVED_METRIC_PREFIX = "__saved_metric__"
DASHBOARD_PREFIX = "__dashboard__"
EVENT_DEF_PREFIX = "__event_def__"


class InvalidStorageReference(Exception):
    pass


class StorageReference(M.Reference[M.ID]):
    def __init__(self, id: str, storage: MitzuStorage, prefix: str):
        super().__init__(None)
        self._id = id
        self.storage = storage
        self.prefix = prefix

    def get_id(self) -> Optional[str]:
        return super().get_id()

    def get_value(self) -> Optional[M.ID]:
        res = self._value_state.get_value()
        if self._id is None:
            return None

        if res is None:
            res = self.storage.mitzu_cache.get(self.prefix + self._id)
            H.LOGGER.debug(f"Restoring storage-ref value: {self.prefix + self._id}")
            self.restore_value(res)
        return res

    def get_value_if_exists(self) -> M.ID:
        return super().get_value_if_exists()

    def store_value(self):
        if self._id is not None:
            self.storage.mitzu_cache.clear(self.prefix + self._id)
        value = self._value_state.get_value()
        if value is not None:
            self.storage.mitzu_cache.put(self.prefix + value.get_id(), value)

    def set_value(self, value: Optional[M.ID]):
        super().set_value(value)
        self.store_value()

    def restore_value(self, value: Optional[M.ID]):
        super().restore_value(value)

    @classmethod
    def from_reference(
        cls,
        reference: M.Reference[M.ID],
        storage: MitzuStorage,
        prefix: str,
    ):
        if isinstance(reference, StorageReference):
            return reference

        ref_id = reference.get_id()
        if ref_id is None:
            return InvalidStorageReference(
                "Trying to convert Reference to StorageReference without an ID."
            )

        res: StorageReference[M.ID] = StorageReference(
            id=ref_id, storage=storage, prefix=prefix
        )
        res.restore_value(reference.get_value())
        return res


def setup_sample_project(storage: MitzuStorage):
    if storage.get_project(SAMPLE_PROJECT_ID) is not None:
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
        storage.set_event_data_table_definition(
            project_id=project.id, definitions=defs, edt_full_name=edt.get_full_name()
        )


class MitzuStorage:
    def __init__(self, mitzu_cache: C.MitzuCache) -> None:
        super().__init__()
        self.mitzu_cache = mitzu_cache

    def set_project(self, project_id: str, project: M.Project):
        for edt in project.event_data_tables:
            edt.project_reference = StorageReference.from_reference(
                edt.project_reference, self, PROJECT_PREFIX
            )

        project._connection_ref = StorageReference.from_reference(
            project._connection_ref, self, CONNECTION_PREFIX
        )
        return self.mitzu_cache.put(PROJECT_PREFIX + project_id, project)

    def get_project(self, project_id: str) -> M.Project:
        project: M.Project = self.mitzu_cache.get(PROJECT_PREFIX + project_id)
        if project is not None:
            definitions: Dict[M.EventDataTable, Dict[str, M.Reference[M.EventDef]]] = {}
            for edt in project.event_data_tables:
                defs = self.get_event_data_table_definition(
                    project_id, edt.get_full_name()
                )
                if defs is not None:
                    definitions[edt] = defs

            project._discovered_project.set_value(
                M.DiscoveredProject(definitions, project)
            )
        return project

    def delete_project(self, project_id: str):
        self.mitzu_cache.clear(PROJECT_PREFIX + project_id)

    def list_projects(self) -> List[str]:
        return self.mitzu_cache.list_keys(PROJECT_PREFIX)

    def set_connection(self, connection_id: str, connection: M.Connection):
        self.mitzu_cache.put(CONNECTION_PREFIX + connection_id, connection)

    def get_connection(self, connection_id: str) -> M.Connection:
        return self.mitzu_cache.get(CONNECTION_PREFIX + connection_id)

    def delete_connection(self, connection_id: str):
        self.mitzu_cache.clear(CONNECTION_PREFIX + connection_id)

    def list_connections(self) -> List[str]:
        return self.mitzu_cache.list_keys(CONNECTION_PREFIX)

    def set_event_data_table_definition(
        self,
        project_id: str,
        edt_full_name: str,
        definitions: Dict[str, M.Reference[M.EventDef]],
    ):
        # Refernece can't be stored so we store the real values
        storage_refs: Dict[str, StorageReference[M.EventDef]] = {}
        for k, ref in definitions.items():
            st_ref = StorageReference.from_reference(
                reference=ref, storage=self, prefix=EVENT_DEF_PREFIX
            )
            storage_refs[k] = st_ref
            st_ref.store_value()

        self.mitzu_cache.put(
            SEPC_PROJECT_PREFIX + project_id + TABLE_DEFIONITION_PREFIX + edt_full_name,
            storage_refs,
        )

    def get_event_data_table_definition(
        self, project_id: str, edt_full_name: str
    ) -> Dict[str, M.Reference[M.EventDef]]:
        res = self.mitzu_cache.get(
            SEPC_PROJECT_PREFIX + project_id + TABLE_DEFIONITION_PREFIX + edt_full_name
        )

        return res

    def delete_event_data_table_definition(self, project_id: str, edt_full_name: str):
        self.mitzu_cache.clear(
            SEPC_PROJECT_PREFIX + project_id + TABLE_DEFIONITION_PREFIX + edt_full_name
        )

    def set_query_result_dataframe(
        self,
        metric_hash: str,
        dataframe: pd.DataFrame,
        expire: float = DEFAULT_EXPIRE_TIME,
    ):
        self.mitzu_cache.put(
            SIMPLE_CHART_PREFIX + metric_hash, dataframe, expire=expire
        )

    def get_query_result_dataframe(
        self,
        metric_hash: str,
    ) -> pd.DataFrame:
        return self.mitzu_cache.get(SIMPLE_CHART_PREFIX + metric_hash)

    def clear_query_result_dataframe(
        self,
        metric_hash: str,
    ):
        self.mitzu_cache.clear(SIMPLE_CHART_PREFIX + metric_hash)

    def set_saved_metric(self, metric_id: str, saved_metric: WM.SavedMetric):
        self.mitzu_cache.put(SAVED_METRIC_PREFIX + metric_id, saved_metric)

    def get_saved_metric(self, metric_id: str) -> WM.SavedMetric:
        res: WM.SavedMetric = self.mitzu_cache.get(SAVED_METRIC_PREFIX + metric_id)
        if res is not None:
            project = self.get_project(res.get_project_id())
            res.restore_project(project)
        return res

    def clear_saved_metric(self, metric_id: str):
        return self.mitzu_cache.clear(SAVED_METRIC_PREFIX + metric_id)

    def list_saved_metrics(self) -> List[str]:
        return self.mitzu_cache.list_keys(SAVED_METRIC_PREFIX)

    def list_dashboards(self) -> List[str]:
        return self.mitzu_cache.list_keys(DASHBOARD_PREFIX)

    def get_dashboard(self, dashboard_id: str) -> WM.Dashboard:
        dashboard: WM.Dashboard = self.mitzu_cache.get(DASHBOARD_PREFIX + dashboard_id)
        if dashboard is not None:
            for dm in dashboard.dashboard_metrics:
                sm = self.get_saved_metric(dm.get_saved_metric_id())
                dm.restore_saved_metric(sm)

        return dashboard

    def set_dashboard(self, dashboard_id: str, dashboard: WM.Dashboard):
        return self.mitzu_cache.put(DASHBOARD_PREFIX + dashboard_id, dashboard)

    def clear_dashboard(self, dashboard_id: str):
        return self.mitzu_cache.clear(DASHBOARD_PREFIX + dashboard_id)
