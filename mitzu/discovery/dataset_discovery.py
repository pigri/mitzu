from datetime import datetime
from typing import List, Dict
from mitzu.common.model import (
    ANY_EVENT_NAME,
    Field,
    DataType,
    EventDataSource,
    EventDef,
    DiscoveredDataset,
)
from mitzu.adapters.generic_adapter import GenericDatasetAdapter


class EventDatasetDiscovery:
    def __init__(
        self,
        source: EventDataSource,
        dataset_adapter: GenericDatasetAdapter,
        end_dt: datetime = None,
        start_dt: datetime = None,
    ):
        self.source = source
        self.dataset_adapter = dataset_adapter
        self.start_dt = start_dt
        self.end_dt = end_dt

    def _get_all_event_fields(
        self,
        event_name: str,
        all_fields: List[Field],
        map_fields: Dict[Field, Dict[str, List[Field]]],
    ) -> List[Field]:
        """Returns the extended field names (a.b.c) for every field that is event_specific"""

        res_fields = all_fields.copy()
        for mf, evt_keys in map_fields.items():
            map_key_field = evt_keys[event_name]
            for mkf in map_key_field:
                mkf._parent = mf
                res_fields.append(mkf)
        return res_fields

    def _get_generic_field_values(self, generic_fields: List[Field]):
        adapter = self.dataset_adapter
        map_field_keys = {}
        map_fields = [gf for gf in generic_fields if gf._type == DataType.MAP]
        for mf in map_fields:
            map_field_keys[mf] = adapter.get_map_field_keys(mf, False)

        all_fields = self._get_all_event_fields(
            ANY_EVENT_NAME, generic_fields, map_field_keys
        )
        return adapter.get_field_enums(all_fields, False)

    def _get_specific_field_values(
        self, specific_fields: List[Field]
    ) -> Dict[str, EventDef]:
        adapter = self.dataset_adapter
        map_fields = [sf for sf in specific_fields if sf._type == DataType.MAP]
        event_names = adapter.get_distinct_event_names()
        event_names.sort()

        map_field_keys: Dict[Field, Dict[str, List[Field]]] = {}

        for mf in map_fields:
            map_field_keys[mf] = adapter.get_map_field_keys(mf, True)

        for event_name in event_names:
            all_fields = self._get_all_event_fields(
                event_name, specific_fields, map_field_keys
            )
        return adapter.get_field_enums(all_fields, True)

    def _get_specific_fields(self, columns: List[Field]):
        res = []
        for spec_col_name in self.source.event_data_table.event_specific_fields:
            res.extend([col for col in columns if col._name.startswith(spec_col_name)])
        return res

    def discover_dataset(self) -> DiscoveredDataset:
        fields = self.dataset_adapter.list_fields()
        fields = [
            f
            for f in fields
            if f._name not in self.source.event_data_table.ignored_fields
        ]

        specific_fields = self._get_specific_fields(fields)
        generic_fields = [c for c in fields if c not in specific_fields]

        generic_prop_vals = self._get_generic_field_values(generic_fields)
        specific_prop_vals = self._get_specific_field_values(specific_fields)

        dd = DiscoveredDataset(
            definitions={**generic_prop_vals, **specific_prop_vals}, source=self.source
        )
        return dd
