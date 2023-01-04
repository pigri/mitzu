from __future__ import annotations

from enum import Enum
from typing import Optional

import dash.development.base_component as bc
import mitzu.model as M
import dash_mantine_components as dmc


METRIC_TYPE_DROPDOWN = "metric-type-dropdown"
METRIC_TYPE_DROPDOWN_OPTION = "metric-type-dropdown-option"


class MetricType(Enum):
    SEGMENTATION = "segmentation"
    CONVERSION = "funnel"
    RETENTION = "retention"
    JOURNEY = "journey"

    @classmethod
    def from_metric(cls, metric: Optional[M.Metric]) -> MetricType:
        if isinstance(metric, M.ConversionMetric):
            return MetricType.CONVERSION
        elif isinstance(metric, M.RetentionMetric):
            return MetricType.RETENTION
        else:
            return MetricType.SEGMENTATION


def from_metric_type(metric_type: MetricType) -> bc.Component:
    return dmc.Select(
        data=[
            {
                "label": m_type.name,
                "value": m_type,
                "disabled": m_type == MetricType.JOURNEY,
            }
            for m_type in MetricType
        ],
        id=METRIC_TYPE_DROPDOWN,
        clearable=False,
        value=metric_type,
        size="xs",
        style={"width": "140px"},
    )
