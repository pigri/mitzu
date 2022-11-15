from dataclasses import dataclass
from enum import Enum, auto
import pandas as pd


TTC_RANGE_1_SEC = 600
TTC_RANGE_2_SEC = 7200
TTC_RANGE_3_SEC = 48 * 3600


X_AXIS_COL = "x"
Y_AXIS_COL = "y"
TEXT_COL = "text"
COLOR_COL = "color"
TOOLTIP_COL = "tooltip"


class SimpleChartType(Enum):
    BAR = auto()
    HORIZONTAL_BAR = auto()
    STACKED_BAR = auto()
    HORIZONTAL_STACKED_BAR = auto()
    LINE = auto()
    STACKED_AREA = auto()
    # HEATMAP = auto()


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
