from __future__ import annotations
import mitzu.common.model as M
from typing import Dict, List, Any
import pandas as pd  # type: ignore

# Final Select Columns
EVENT_NAME_ALIAS_COL = "_event_type"
DATETIME_COL = "_datetime"
GROUP_COL = "_group"
USER_COUNT_COL = "_unique_user_count"
EVENT_COUNT_COL = "_event_count"
CVR_COL = "_conversion_rate"


# CTE Colmns
CTE_USER_ID_ALIAS_COL = "_cte_user_id"
CTE_DATETIME_COL = "_cte_datetime"
CTE_GROUP_COL = "_cte_group"


class GenericDatasetAdapter:
    def __init__(self, source: M.EventDataSource):
        self.source = source

    def __del__(self):
        forwarder = self.source.connection.ssh_tunnel_forwarder
        if forwarder is not None and forwarder.is_alive:
            forwarder.close()

    def _create_ssh_tunnel(self):
        forwarder = self.source.connection.ssh_tunnel_forwarder
        if forwarder is not None and not forwarder.is_alive:
            forwarder.start()

    def execute_query(self, query: Any) -> pd.DataFrame:
        raise NotImplementedError()

    def validate_source(self):
        raise NotImplementedError()

    def list_fields(self, event_data_table: M.EventDataTable) -> List[M.Field]:
        raise NotImplementedError()

    def get_map_field_keys(
        self,
        event_data_table: M.EventDataTable,
        map_field: M.Field,
        event_specific: bool,
    ) -> Dict[str, List[M.Field]]:
        raise NotImplementedError()

    def get_distinct_event_names(self, event_data_table: M.EventDataTable) -> List[str]:
        raise NotImplementedError()

    def get_field_enums(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> Dict[str, M.EventDef]:
        raise NotImplementedError()

    def get_conversion_sql(self, metric: M.ConversionMetric) -> str:
        raise NotImplementedError()

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        raise NotImplementedError()

    def get_segmentation_sql(self, metric: M.SegmentationMetric) -> str:
        raise NotImplementedError()

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        raise NotImplementedError()

    def _fix_col_index(self, index: int, col_name: str):
        return col_name + f"_{index}"
