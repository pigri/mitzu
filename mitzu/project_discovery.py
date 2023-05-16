from __future__ import annotations

from typing import Callable, Dict, Optional

import mitzu.model as M
from mitzu.helper import LOGGER
import traceback


class ProjectDiscoveryError(Exception):
    pass


class ProjectDiscovery:
    def __init__(self, project: M.Project, callback: Optional[Callable] = None):
        self.project = project
        self.callback = callback

    def _create_event_field_def_references(
        self,
        event_data_table: M.EventDataTable,
        specific: Dict[str, M.EventDef],
    ) -> Dict[str, M.Reference[M.EventDef]]:
        res: Dict[str, M.Reference[M.EventDef]] = {}
        for evt_name, spec_evt_def in specific.items():
            new_def = M.EventDef(
                _event_data_table=event_data_table,
                _event_name=evt_name,
                _fields=spec_evt_def._fields,
            )
            res[evt_name] = M.Reference.create_from_value(new_def)

        return res

    def discover_project(self, progress_bar: bool = False) -> M.DiscoveredProject:
        definitions: Dict[M.EventDataTable, Dict[str, M.Reference[M.EventDef]]] = {}

        self.project.validate()
        tables = self.project.event_data_tables
        adapter = self.project.get_adapter()
        errors = {}
        for ed_table in tables:
            defs = {}
            try:
                LOGGER.debug(f"Discovering {ed_table.get_full_name()}")
                fields = adapter.list_fields(event_data_table=ed_table)
                fields = [
                    f for f in fields if f._get_name() not in ed_table.ignored_fields
                ]

                field_enums = adapter.get_field_enums(ed_table, fields)
                defs = self._create_event_field_def_references(
                    ed_table,
                    field_enums,
                )
                definitions[ed_table] = defs
            except Exception as exc:
                LOGGER.error(f"{ed_table.table_name} failed to discover: {str(exc)}")
                errors[ed_table.table_name] = exc

            if self.callback is not None:
                self.callback(ed_table, defs, errors.get(ed_table.table_name))

        dd = M.DiscoveredProject(
            definitions=definitions,
            project=self.project,
        )

        if len(errors) > 0:
            traceback.print_exception(list(errors.values())[0])
            LOGGER.warning(f"Finished discovery with {len(errors)} errors.")
        else:
            LOGGER.info("Successfully finished dataset discovery.")
        return dd
