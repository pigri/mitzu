from __future__ import annotations

from typing import Dict, List, Optional, Callable

import mitzu.model as M
from mitzu.helper import LOGGER
from tqdm import tqdm
import sys
import traceback


class ProjectDiscoveryError(Exception):
    pass


class ProjectDiscovery:
    def __init__(self, project: M.Project, callback: Optional[Callable] = None):
        self.project = project
        self.callback = callback

    def _get_field_values(
        self,
        ed_table: M.EventDataTable,
        specific_fields: List[M.Field],
        event_specific: bool,
    ) -> Dict[str, M.EventDef]:
        return self.project.get_adapter().get_field_enums(
            event_data_table=ed_table,
            fields=specific_fields,
            event_specific=event_specific,
        )

    def _get_specific_fields(
        self, ed_table: M.EventDataTable, all_fields: List[M.Field]
    ) -> List[M.Field]:
        res = []
        if ed_table.event_name_field is None:
            return []
        if ed_table.event_specific_fields is None:
            res = [
                f
                for f in all_fields
                if (
                    f
                    not in (
                        ed_table.event_name_field,
                        ed_table.event_time_field,
                        ed_table.user_id_field,
                    )
                    and f._get_name() not in ed_table.ignored_fields
                )
            ]
        else:
            for spec_field in ed_table.event_specific_fields:
                res.extend(
                    [
                        f
                        for f in all_fields
                        if f._get_name().startswith(spec_field._get_name())
                    ]
                )
        return res

    def _copy_gen_field_def_to_spec(
        self, spec_event_name: str, gen_evt_field_def: M.EventFieldDef
    ):
        return M.EventFieldDef(
            _event_name=spec_event_name,
            _field=gen_evt_field_def._field,
            _event_data_table=gen_evt_field_def._event_data_table,
            _enums=gen_evt_field_def._enums,
        )

    def _merge_generic_and_specific_definitions(
        self,
        event_data_table: M.EventDataTable,
        generic: M.EventDef,
        specific: Dict[str, M.EventDef],
    ) -> Dict[str, M.Reference[M.EventDef]]:
        res: Dict[str, M.Reference[M.EventDef]] = {}
        for evt_name, spec_evt_def in specific.items():
            copied_gen_fields = {
                field: self._copy_gen_field_def_to_spec(evt_name, field_def)
                for field, field_def in generic._fields.items()
            }

            new_def = M.EventDef(
                _event_data_table=event_data_table,
                _event_name=evt_name,
                _fields={**spec_evt_def._fields, **copied_gen_fields},
            )
            res[evt_name] = M.Reference.create_from_value(new_def)

        return res

    def discover_project(self, progress_bar: bool = False) -> M.DiscoveredProject:
        definitions: Dict[M.EventDataTable, Dict[str, M.Reference[M.EventDef]]] = {}

        self.project.validate()
        tables = self.project.event_data_tables
        if progress_bar:
            tables = tqdm(
                tables, leave=False, file=sys.stdout, desc="Discovering datasets"
            )
        errors = {}
        for ed_table in tables:
            defs = {}
            try:
                LOGGER.debug(f"Discovering {ed_table.get_full_name()}")
                fields = self.project.get_adapter().list_fields(
                    event_data_table=ed_table
                )
                specific_fields = self._get_specific_fields(ed_table, fields)

                generic_fields = [c for c in fields if c not in specific_fields]
                generic_field_values = self._get_field_values(
                    ed_table, generic_fields, False
                )
                if M.ANY_EVENT_NAME not in generic_field_values.keys():
                    any_event_field_values = M.EventDef(
                        _event_name=M.ANY_EVENT_NAME,
                        _fields={},
                        _event_data_table=ed_table,
                    )
                else:
                    any_event_field_values = generic_field_values[M.ANY_EVENT_NAME]

                event_specific_field_values = self._get_field_values(
                    ed_table, specific_fields, True
                )
                defs = self._merge_generic_and_specific_definitions(
                    ed_table,
                    any_event_field_values,
                    event_specific_field_values,
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
