from __future__ import annotations

from abc import ABC
from datetime import datetime
from typing import Any, Dict, List

import mitzu.model as M
import pandas as pd

# Final Select Columns
EVENT_NAME_ALIAS_COL = "_event_name"
DATETIME_COL = "_datetime"
GROUP_COL = "_group"
USER_COUNT_COL = "_unique_user_count"
EVENT_COUNT_COL = "_event_count"
CVR_COL = "_conversion_rate"


# CTE Colmns
CTE_USER_ID_ALIAS_COL = "_cte_user_id"
CTE_DATETIME_COL = "_cte_datetime"
CTE_GROUP_COL = "_cte_group"


class GenericDatasetAdapter(ABC):
    def __init__(self, source: M.EventDataSource):
        self.source = source

    def execute_query(self, query: Any) -> pd.DataFrame:
        raise NotImplementedError()

    def list_fields(
        self, event_data_table: M.EventDataTable, config: M.DatasetDiscoveryConfig
    ) -> List[M.Field]:
        raise NotImplementedError()

    def get_map_field_keys(
        self,
        event_data_table: M.EventDataTable,
        map_field: M.Field,
        event_specific: bool,
    ) -> Dict[str, List[M.Field]]:
        raise NotImplementedError()

    def get_distinct_event_names(
        self, event_data_table: M.EventDataTable, config: M.DatasetDiscoveryConfig
    ) -> List[str]:
        raise NotImplementedError()

    def get_field_enums(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
        config: M.DatasetDiscoveryConfig,
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

    def test_connection(self):
        raise NotImplementedError()

    def get_last_event_times(
        self,
    ) -> Dict[str, datetime]:
        raise NotImplementedError()
