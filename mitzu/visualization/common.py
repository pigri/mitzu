from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
import pandas as pd
from typing import Any, Union, Optional, Callable
import mitzu.model as M


TTC_RANGE_1_SEC = 600
TTC_RANGE_2_SEC = 7200
TTC_RANGE_3_SEC = 48 * 3600


X_AXIS_COL = "x"
Y_AXIS_COL = "y"
TEXT_COL = "_text"
COLOR_COL = "_color"
TOOLTIP_COL = "_tooltip"


def retention_x_tick_label(val: int, metric: M.Metric) -> str:
    if isinstance(metric, M.RetentionMetric):
        return f"{val} to {val+ metric._retention_window.value} {metric._retention_window.period.name.lower()}"
    return str(val)


class SimpleChartType(Enum):
    BAR = auto()
    HORIZONTAL_BAR = auto()
    STACKED_BAR = auto()
    HORIZONTAL_STACKED_BAR = auto()
    LINE = auto()
    STACKED_AREA = auto()
    HEATMAP = auto()

    @classmethod
    def parse(cls, value: Union[str, SimpleChartType]) -> SimpleChartType:
        if type(value) == SimpleChartType:
            return value
        elif type(value) == str:
            for key, enm in SimpleChartType._member_map_.items():
                if key.lower() == value.lower():
                    return SimpleChartType[key]
            raise ValueError(
                f"Unknown chart type {value} supported are {[k.lower() for k in SimpleChartType._member_names_]}"
            )
        else:
            raise ValueError("Parse should only be str or SimpleChartType")


@dataclass(frozen=True)
class SimpleChart:

    title: str
    x_axis_label: str
    y_axis_label: str
    color_label: str
    yaxis_ticksuffix: str
    hover_mode: str
    chart_type: SimpleChartType
    dataframe: pd.DataFrame
    x_axis_tick_labels_func: Optional[Callable[[Any, M.Metric], Any]] = None
    y_axis_tick_labels_func: Optional[Callable[[Any, M.Metric], Any]] = None
