from __future__ import annotations
from datetime import datetime
from typing import List, Dict
import mitzu.common.model as M


class EventDatasourceDiscovery:
    def __init__(
        self,
        source: M.EventDataSource,
        end_dt: datetime = None,
        start_dt: datetime = None,
    ):
        self.source = source
        self.start_dt = start_dt
        self.end_dt = end_dt

    def _get_all_event_fields(
        self,
        event_name: str,
        all_fields: List[M.Field],
        map_fields: Dict[M.Field, Dict[str, List[M.Field]]],
    ) -> List[M.Field]:
        """Returns the extended field names (a.b.c) for every field that is event_specific"""

        res_fields = all_fields.copy()
        for mf, evt_keys in map_fields.items():
            map_key_field = evt_keys[event_name]
            for mkf in map_key_field:
                mkf._parent = mf
                res_fields.append(mkf)
        return res_fields

    def _get_generic_field_values(
        self, ed_table: M.EventDataTable, generic_fields: List[M.Field]
    ) -> M.EventDef:
        adapter = self.source.adapter
        map_field_keys = {}
        map_fields = [gf for gf in generic_fields if gf._type == M.DataType.MAP]
        for mf in map_fields:
            map_field_keys[mf] = adapter.get_map_field_keys(
                event_data_table=ed_table, map_field=mf, event_specific=False
            )

        all_fields = self._get_all_event_fields(
            M.ANY_EVENT_NAME, generic_fields, map_field_keys
        )
        return adapter.get_field_enums(
            event_data_table=ed_table, fields=all_fields, event_specific=False
        )[M.ANY_EVENT_NAME]

    def _get_specific_field_values(
        self, ed_table: M.EventDataTable, specific_fields: List[M.Field]
    ) -> Dict[str, M.EventDef]:
        adapter = self.source.adapter
        map_fields = [sf for sf in specific_fields if sf._type == M.DataType.MAP]
        event_names = adapter.get_distinct_event_names(event_data_table=ed_table)
        event_names.sort()

        map_field_keys: Dict[M.Field, Dict[str, List[M.Field]]] = {}

        for mf in map_fields:
            map_field_keys[mf] = adapter.get_map_field_keys(
                event_data_table=ed_table, map_field=mf, event_specific=True
            )

        for event_name in event_names:
            all_fields = self._get_all_event_fields(
                event_name, specific_fields, map_field_keys
            )
        return adapter.get_field_enums(
            event_data_table=ed_table, fields=all_fields, event_specific=True
        )

    def _get_specific_fields(self, ed_table: M.EventDataTable, columns: List[M.Field]):
        res = []
        for spec_col_name in ed_table.event_specific_fields:
            res.extend([col for col in columns if col._name.startswith(spec_col_name)])
        return res

    def discover_datasource(self) -> M.DiscoveredEventDataSource:
        generic_definitions: Dict[M.EventDataTable, M.EventDef] = {}
        event_specific_definitions: Dict[M.EventDataTable, Dict[str, M.EventDef]] = {}
        for ed_table in self.source.event_data_tables:
            fields = self.source.adapter.list_fields(event_data_table=ed_table)
            fields = [f for f in fields if f._name not in ed_table.ignored_fields]
            specific_fields = self._get_specific_fields(ed_table, fields)
            generic_fields = [c for c in fields if c not in specific_fields]

            generic_definitions[ed_table] = self._get_generic_field_values(
                ed_table, generic_fields
            )

            event_specific_definitions[ed_table] = self._get_specific_field_values(
                ed_table, specific_fields
            )

        dd = M.DiscoveredEventDataSource(
            generic_definitions=generic_definitions,
            event_specific_definitions=event_specific_definitions,
            source=self.source,
        )
        return dd
