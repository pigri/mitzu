from dataclasses import dataclass
import mitzu.webapp.storage as S
from typing import Callable, Dict, List, Optional
import mitzu.model as M


class EventServiceException(Exception):
    pass


@dataclass
class EventsService:

    storage: S.MitzuStorage

    def list_all_projects(self) -> List[M.Project]:
        project_ids = self.storage.list_projects()
        return [
            self.storage.get_project(pid) for pid in project_ids
        ]  # todo move this logic to storage

    def get_project_definition(
        self, project_id: str
    ) -> Dict[M.EventDataTable, Dict[str, M.Reference[M.EventDef]]]:
        dp = self.storage.get_project(project_id)._discovered_project.get_value()
        if dp is None:
            raise EventServiceException(f"Missing discovered project for {project_id}")
        return dp.definitions

    def discover_project(
        self,
        project_id: str,
        callback: Callable[
            [
                M.EventDataTable,
                Dict[str, M.Reference[M.EventDef]],
                Optional[Exception],
                int,
                int,
            ],
            None,
        ],
    ) -> Dict[M.EventDataTable, Dict[str, M.Reference[M.EventDef]]]:
        project = self.storage.get_project(project_id)
        edt_count = len(project.event_data_tables)
        edts = []

        def edt_callback(
            edt: M.EventDataTable,
            defs: Dict[str, M.Reference[M.EventDef]],
            exc: Optional[Exception],
        ):
            if exc is None:
                self.storage.set_event_data_table_definition(
                    project_id, edt_full_name=edt.get_full_name(), definitions=defs
                )
            edts.append(edt)
            callback(edt, defs, exc, len(edts), edt_count)

        discovered_project = project.discover_project(False, edt_callback)
        self.storage.set_project(
            project_id=discovered_project.project.id,
            project=discovered_project.project,
        )

        return discovered_project.definitions
