from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

import mitzu.model as M
import mitzu.webapp.cache as C
import mitzu.webapp.model as WM
from mitzu.samples.data_ingestion import create_and_ingest_sample_project
from datetime import datetime

SAMPLE_PROJECT_NAME = "sample_project"

PROJECT_PREFIX = "__project__"
CONNECTION_PREFIX = "__conn__"
SEPC_PROJECT_PREFIX = "__spec_project__"
TABLE_DEFIONITION_PREFIX = "__table_definition__"
TABLE_PREFIX = "__table__"


DEFAULT_EXPIRE_TIME = 600  # 10 minutes

PROJECT_PREFIX = "__project__"
CONNECTION_PREFIX = "__conn__"
SEPC_PROJECT_PREFIX = "__spec_project__"
TABLE_DEFIONITION_PREFIX = "__table_definition__"
TABLE_PREFIX = "__table__"
SIMPLE_CHART_PREFIX = "__simple_chart__"
SAVED_METRIC_PREFIX = "__saved_metric__"
DASHBOARD_PREFIX = "__dashboard__"


def create_sample_project() -> M.DiscoveredProject:
    connection = M.Connection(
        connection_name="Sample project",
        connection_type=M.ConnectionType.SQLITE,
    )
    project = create_and_ingest_sample_project(
        connection, event_count=20000, number_of_users=100
    )
    return project.discover_project()


class MitzuStorage:
    def __init__(self, mitzu_cache: C.MitzuCache) -> None:
        super().__init__()
        self.sample_project: Optional[M.DiscoveredProject] = None
        self.mitzu_cache = mitzu_cache

    def get_discovered_project(self, project_id: str) -> Optional[M.DiscoveredProject]:
        if project_id == SAMPLE_PROJECT_NAME:
            if self.sample_project is None:
                self.sample_project = create_sample_project()
            return self.sample_project
        else:
            project = self.get_project(project_id)
            tbl_defs = project.event_data_tables
            definitions: Dict[M.EventDataTable, Dict[str, M.EventDef]] = {}

            for edt in tbl_defs:
                defs = self.get_event_data_table_definition(
                    project_id, edt.get_full_name()
                )
                definitions[edt] = defs if defs is not None else {}
            dp = M.DiscoveredProject(definitions, project)
            return dp

    def set_project(self, project_id: str, project: M.Project):
        return self.mitzu_cache.put(PROJECT_PREFIX + project_id, project)

    def get_project(self, project_id: str) -> M.Project:
        return self.mitzu_cache.get(PROJECT_PREFIX + project_id)

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
        self, project_id: str, edt_full_name: str, definitions: Dict[str, M.EventDef]
    ):
        self.mitzu_cache.put(
            SEPC_PROJECT_PREFIX + project_id + TABLE_DEFIONITION_PREFIX + edt_full_name,
            definitions,
        )

    def get_event_data_table_definition(
        self, project_id: str, edt_full_name: str
    ) -> Dict[str, M.EventDef]:
        return self.mitzu_cache.get(
            SEPC_PROJECT_PREFIX + project_id + TABLE_DEFIONITION_PREFIX + edt_full_name,
            {},
        )

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
        return self.mitzu_cache.get(SAVED_METRIC_PREFIX + metric_id)

    def clear_saved_metric(self, metric_id: str):
        return self.mitzu_cache.clear(SAVED_METRIC_PREFIX + metric_id)

    def list_saved_metrics(self) -> List[str]:
        return self.mitzu_cache.list_keys(SAVED_METRIC_PREFIX)

    def list_dashboards(self) -> List[str]:
        return self.mitzu_cache.list_keys(DASHBOARD_PREFIX)

    def get_dashboard(self, dashboard_id: str) -> WM.Dashboard:
        dashboard: WM.Dashboard = self.mitzu_cache.get(DASHBOARD_PREFIX + dashboard_id)
        if dashboard is None:
            return None
        for dm in dashboard.dashboard_metrics:
            if dm.saved_metric is None:
                dm.saved_metric = self.get_saved_metric(dm.saved_metric_id)
        return dashboard

    def set_dashboard(self, dashboard_id: str, dashboard: WM.Dashboard):
        metrics = [
            WM.DashboardMetric(
                saved_metric_id=dm.saved_metric_id,
                x=dm.x,
                y=dm.y,
                width=dm.width,
                height=dm.height,
                saved_metric=None,  # we don't want to save the SaveMetric to the dashboard object
            )
            for dm in dashboard.dashboard_metrics
        ]
        new_dashboard = WM.Dashboard(
            name=dashboard.name,
            id=dashboard.id,
            dashboard_metrics=metrics,
            created_on=dashboard.created_on,
            last_modified=datetime.now(),
        )
        return self.mitzu_cache.put(DASHBOARD_PREFIX + dashboard_id, new_dashboard)

    def clear_dashboard(self, dashboard_id: str):
        return self.mitzu_cache.clear(DASHBOARD_PREFIX + dashboard_id)
