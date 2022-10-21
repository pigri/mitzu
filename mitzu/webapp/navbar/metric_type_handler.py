from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

import dash.development.base_component as bc
import mitzu.model as M
from dash import dcc, html
from mitzu.webapp.helper import value_to_label

METRIC_TYPE_DROPDOWN = "metric-type-dropdown"
METRIC_TYPE_DROPDOWN_OPTION = "metric-type-dropdown-option"


class MetricType(Enum):
    SEGMENTATION = "segmentation"
    CONVERSION = "conversion"
    RETENTION = "retention"
    JOURNEY = "journey"
    REVENUE = "revenue"

    @classmethod
    def from_metric(cls, metric: Optional[M.Metric]) -> MetricType:
        if isinstance(metric, M.ConversionMetric):
            return MetricType.CONVERSION
        else:
            return MetricType.SEGMENTATION


TYPES: Dict[MetricType, str] = {
    MetricType.SEGMENTATION: "bi bi-graph-up",
    MetricType.CONVERSION: "bi bi-filter-square",
    MetricType.RETENTION: "bi bi-arrow-clockwise",
    MetricType.REVENUE: "bi bi-currency-exchange",
    MetricType.JOURNEY: "bi bi-bezier2",
}

DEF_STYLE = {"font-size": 15, "padding-left": 10}


def from_metric_type(metric_type: MetricType) -> bc.Component:
    return dcc.Dropdown(
        options=[
            {
                "label": html.Div(
                    [
                        html.I(className=css_class),
                        html.Div(value_to_label(val.value), style=DEF_STYLE),
                    ],
                    className=METRIC_TYPE_DROPDOWN_OPTION,
                ),
                "value": val.value,
                "disabled": val not in [MetricType.SEGMENTATION, MetricType.CONVERSION],
            }
            for val, css_class in TYPES.items()
        ],
        id=METRIC_TYPE_DROPDOWN,
        className=METRIC_TYPE_DROPDOWN,
        clearable=False,
        value=metric_type.value,
        searchable=False,
        style={"border-radius": "5px"},
    )


def from_all_inputs(all_inputs: Dict[str, Any]) -> MetricType:
    return MetricType(all_inputs.get(METRIC_TYPE_DROPDOWN))
