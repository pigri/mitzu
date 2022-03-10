from __future__ import annotations
import services.common.model as M
from typing import Dict, List
import pandas as pd


class GenericDatasetAdapter:
    def __init__(self, source: M.EventDataSource):
        self.source = source

    def validate_source(self):
        raise NotImplementedError()

    def list_fields(self) -> List[M.Field]:
        """Should return all fields that data set has.
            Every sub field is listed for the struct types.
            Map types are listed MAP. Keys in the MAP types have to exploded to be supported.

        Returns:
            List[Field]: list of fields including struct subfields. Map types are listed as DataType.MAP.
        """
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

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        raise NotImplementedError()

    def get_segmentation_sql(self, metric: M.SegmentationMetric) -> str:
        raise NotImplementedError()

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        raise NotImplementedError()
