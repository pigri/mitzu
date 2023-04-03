from __future__ import annotations

from enum import Enum, auto
from typing import Optional

import dash.development.base_component as bc
import mitzu.model as M
import dash_mantine_components as dmc


METRIC_TYPE_DROPDOWN = "metric-type-dropdown"


class MetricType(Enum):
    SEGMENTATION = auto()
    CONVERSION = auto()
    RETENTION = auto()
    USER_JOURNEYS = auto()
    REVENUE = auto()
    AB_TEST_ANALYSIS = auto()
    COHORTS = auto()
    CHURN_ANALYSIS = auto()

    @classmethod
    def from_metric(cls, metric: Optional[M.Metric]) -> MetricType:
        if isinstance(metric, M.ConversionMetric):
            return MetricType.CONVERSION
        elif isinstance(metric, M.RetentionMetric):
            return MetricType.RETENTION
        else:
            return MetricType.SEGMENTATION

    @classmethod
    def parse(cls, val_str: str) -> MetricType:
        return MetricType[val_str.upper()]


def from_metric_type(metric_type: MetricType) -> bc.Component:
    return dmc.Select(
        data=[
            {
                "label": m_type.name.upper().replace("_", " "),
                "value": m_type.name.upper(),
                "disabled": m_type
                not in (
                    MetricType.SEGMENTATION,
                    MetricType.RETENTION,
                    MetricType.CONVERSION,
                ),
            }
            for m_type in MetricType
        ],
        id=METRIC_TYPE_DROPDOWN,
        clearable=False,
        value=metric_type.name.upper(),
        size="xs",
        style={"width": "140px"},
    )
