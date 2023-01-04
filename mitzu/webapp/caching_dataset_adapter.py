import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
from typing import Any, List, Dict
import pandas as pd
import mitzu.webapp.cache as CA
import hashlib

QUERY_PREFIX = "__QUERY__"
METRIC_PREFIX = "__METRIC__"


class CachingDatasetAdapter(GA.GenericDatasetAdapter):
    def __init__(
        self,
        adapter: GA.GenericDatasetAdapter,
        cache: CA.MitzuCache,
        expire: float = 600,
    ):
        self._adapter = adapter
        self._cache = cache
        self._expire = expire

    def execute_query(self, query: Any) -> pd.DataFrame:
        key = str(hashlib.md5(bytes(query)))
        pdf = self._cache.get(QUERY_PREFIX + key)
        if pdf is None:
            pdf = self._adapter.execute_query(query)
            self._cache.put(key, pdf, self._expire)
        return pdf

    def list_fields(self, event_data_table: M.EventDataTable) -> List[M.Field]:
        return self._adapter.list_fields(event_data_table)

    def get_distinct_event_names(self, event_data_table: M.EventDataTable) -> List[str]:

        return self._adapter.get_distinct_event_names(event_data_table)

    def get_field_enums(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> Dict[str, M.EventDef]:
        return self._adapter.get_field_enums(event_data_table, fields, event_specific)

    def get_conversion_sql(self, metric: M.ConversionMetric) -> str:
        return self._adapter.get_conversion_sql(metric)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        key = hashlib.md5(metric.get_sql().encode()).hexdigest()
        pdf = self._cache.get(METRIC_PREFIX + key)
        if pdf is None:
            pdf = self._adapter.get_conversion_df(metric)
            self._cache.put(key, pdf, self._expire)
        return pdf

    def get_segmentation_sql(self, metric: M.SegmentationMetric) -> str:
        return self._adapter.get_segmentation_sql(metric)

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        key = hashlib.md5(metric.get_sql().encode()).hexdigest()
        pdf = self._cache.get(METRIC_PREFIX + key)
        if pdf is None:
            pdf = self._adapter.get_segmentation_df(metric)
            self._cache.put(key, pdf, self._expire)
        return pdf

    def get_retention_sql(self, metric: M.RetentionMetric) -> str:
        return self._adapter.get_retention_sql(metric)

    def get_retention_df(self, metric: M.RetentionMetric) -> pd.DataFrame:
        key = hashlib.md5(metric.get_sql().encode()).hexdigest()
        pdf = self._cache.get(METRIC_PREFIX + key)
        if pdf is None:
            pdf = self._adapter.get_retention_df(metric)
            self._cache.put(key, pdf, self._expire)
        return pdf

    def test_connection(self):
        self._adapter.test_connection()

    def stop_current_execution(self):
        self._adapter.stop_current_execution()
