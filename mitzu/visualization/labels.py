from __future__ import annotations

import mitzu.model as M

import mitzu.visualization.common as C
from typing import Any


def conv_time_suffix(max_ttc: int) -> str:
    return (
        "secs"
        if max_ttc <= C.TTC_RANGE_1_SEC
        else "mins"
        if max_ttc <= C.TTC_RANGE_2_SEC
        else "hours"
        if max_ttc <= C.TTC_RANGE_3_SEC
        else "days"
    )


def agg_type_label(agg_type: M.AggType, agg_param: Any = None):
    if agg_type == M.AggType.CONVERSION:
        return "Conversion rate"
    if agg_type == M.AggType.RETENTION_RATE:
        return "Retention rate"
    if agg_type == M.AggType.COUNT_UNIQUE_USERS:
        return "Unique users"
    if agg_type == M.AggType.COUNT_EVENTS:
        return "Event count"
    if agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
        return "Avg. time to convert"
    if agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
        return f"P{agg_param} time to convert"
