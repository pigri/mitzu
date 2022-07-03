from __future__ import annotations

from typing import Dict, List

import mitzu.model as M


class EventDatasourceDiscovery:
    def __init__(self, source: M.EventDataSource):
        self.source = source

    def _get_field_values(
        self,
        ed_table: M.EventDataTable,
        specific_fields: List[M.Field],
        event_specific: bool,
    ) -> Dict[str, M.EventDef]:
        return self.source.adapter.get_field_enums(
            event_data_table=ed_table,
            fields=specific_fields,
            event_specific=event_specific,
        )

    def _get_specific_fields(
        self, ed_table: M.EventDataTable, all_fields: List[M.Field]
    ):
        res = []
        for spec_field in ed_table.event_specific_fields:
            res.extend([f for f in all_fields if f._get_name().startswith(spec_field)])
        return res

    def _copy_gen_field_def_to_spec(
        self, spec_event_name: str, gen_evt_field_def: M.EventFieldDef
    ):
        return M.EventFieldDef(
            _event_name=spec_event_name,
            _field=gen_evt_field_def._field,
            _source=gen_evt_field_def._source,
            _event_data_table=gen_evt_field_def._event_data_table,
            _enums=gen_evt_field_def._enums,
        )

    def _merge_generic_and_specific_definitions(
        self,
        source: M.EventDataSource,
        event_data_table: M.EventDataTable,
        generic: M.EventDef,
        specific: Dict[str, M.EventDef],
    ) -> Dict[str, M.EventDef]:
        res: Dict[str, M.EventDef] = {}
        for evt_name, spec_evt_def in specific.items():
            copied_gen_fields = {
                field: self._copy_gen_field_def_to_spec(evt_name, field_def)
                for field, field_def in generic._fields.items()
            }

            new_def = M.EventDef(
                _source=source,
                _event_data_table=event_data_table,
                _event_name=evt_name,
                _fields={**spec_evt_def._fields, **copied_gen_fields},
            )
            res[evt_name] = new_def

        return res

    def flatten_fields(self, fields: List[M.Field]) -> List[M.Field]:
        res = []
        for f in fields:
            if f._type.is_complex():
                if f._sub_fields is not None:
                    res.extend(self.flatten_fields(list(f._sub_fields)))
            else:
                res.append(f)
        return res

    def discover_datasource(self) -> M.DiscoveredEventDataSource:
        definitions: Dict[M.EventDataTable, Dict[str, M.EventDef]] = {}

        for ed_table in self.source.event_data_tables:
            print(f"Discovering {ed_table.table_name}")
            fields = self.source.adapter.list_fields(event_data_table=ed_table)
            fields = self.flatten_fields(fields)

            specific_fields = self._get_specific_fields(ed_table, fields)
            generic_fields = [c for c in fields if c not in specific_fields]
            generic_field_values = self._get_field_values(
                ed_table, generic_fields, False
            )[M.ANY_EVENT_NAME]
            event_specific_field_values = self._get_field_values(
                ed_table, specific_fields, True
            )
            definitions[ed_table] = self._merge_generic_and_specific_definitions(
                self.source,
                ed_table,
                generic_field_values,
                event_specific_field_values,
            )

        dd = M.DiscoveredEventDataSource(
            definitions=definitions,
            source=self.source,
        )

        return dd
