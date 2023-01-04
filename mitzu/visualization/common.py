from __future__ import annotations

from dataclasses import dataclass
import pandas as pd
from typing import Any, Callable, Optional
import mitzu.model as M


TTC_RANGE_1_SEC = 600
TTC_RANGE_2_SEC = 7200
TTC_RANGE_3_SEC = 48 * 3600


X_AXIS_COL = "x"
Y_AXIS_COL = "y"
TEXT_COL = "_text"
COLOR_COL = "_color"
TOOLTIP_COL = "_tooltip"


def retention_period_label(val: int, metric: M.Metric) -> str:
    if isinstance(metric, M.RetentionMetric):
        return f"{val} to {val+ metric._retention_window.value} {metric._retention_window.period.name.lower()}"
    return str(val)


def fix_date_label(val: pd.Timestamp, metric: M.Metric) -> str:
    if metric._time_group not in (
        M.TimeGroup.SECOND,
        M.TimeGroup.MINUTE,
        M.TimeGroup.HOUR,
    ):
        res = str(val.date())
        if metric._chart_type == M.SimpleChartType.HEATMAP:
            res = res + "."
        return res
    return val


@dataclass(frozen=True)
class SimpleChart:

    title: str
    x_axis_label: str
    y_axis_label: str
    color_label: str
    yaxis_ticksuffix: str
    hover_mode: str
    chart_type: M.SimpleChartType
    dataframe: pd.DataFrame
    x_axis_labels_func: Optional[Callable[[Any, M.Metric], Any]] = None
    y_axis_labels_func: Optional[Callable[[Any, M.Metric], Any]] = None
    color_labels_func: Optional[Callable[[Any, M.Metric], Any]] = None
