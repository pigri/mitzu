from __future__ import annotations

from typing import List, Optional, Dict
import mitzu.webapp.cache as C

import mitzu.model as M
from mitzu.samples.data_ingestion import create_and_ingest_sample_project

SAMPLE_PROJECT_NAME = "sample_project"

PROJECT_PREFIX = "__project__"
CONNECTION_PREFIX = "__conn__"
SEPC_PROJECT_PREFIX = "__spec_project__"
TABLE_DEFIONITION_PREFIX = "__table_definition__"
TABLE_PREFIX = "__table__"


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

            return M.DiscoveredProject(definitions, project)

    def set_project(self, project_id: str, project: M.Project):
        # TBD Project now have project_id
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
