from __future__ import annotations
import services.common.model as M
from typing import Dict, List
import pandas as pd  # type: ignore


DATETIME_COL = "datetime"
GROUP_COL = "group"
USER_COUNT_COL = "unique_user_count"
EVENT_COUNT_COL = "event_count"
CVR_COL = "conversion_rate"
PERCENTILE_50_COL = "p50_conv_time"
PERCENTILE_95_COL = "p95_conv_time"


class GenericDatasetAdapter:
    def __init__(self, source: M.EventDataSource):
        self.source = source

    def _fix_col_index(self, index: int, col_name: str):
        return col_name + f"_{index}"

    def validate_source(self):
        raise NotImplementedError()

    def list_fields(self) -> List[M.Field]:
        raise NotImplementedError()

    def get_map_field_keys(
        self, map_field: M.Field, event_specific: bool
    ) -> Dict[str, List[M.Field]]:
        raise NotImplementedError()

    def get_distinct_event_names(self) -> List[str]:
        raise NotImplementedError()

    def get_field_enums(
        self, fields: List[M.Field], event_specific: bool
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
