from __future__ import annotations

from datetime import datetime, timedelta
from typing import cast

import mitzu.common.model as M

MAX_TITLE_LENGTH = 80
DT_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"


def format_date(metric: M.Metric, dt: datetime, adjust_rounding: int = 0) -> str:
    if metric._time_group in [M.TimeGroup.HOUR, M.TimeGroup.MINUTE, M.TimeGroup.SECOND]:
        return dt.strftime(DT_FORMAT)
    else:
        return (dt + timedelta(days=adjust_rounding)).strftime(DATE_FORMAT)


def get_timeframe_str(metric: M.Metric) -> str:
    if metric._config.start_dt is None:
        if metric._config.end_dt is None:
            return f"latest {metric._lookback_days} days"
        else:
            return f"last <b>{metric._lookback_days}</b> days before <b>{format_date(metric,metric._end_dt,1)}</b>"
    else:
        return (
            f"between <b>{format_date(metric,metric._start_dt)}</b> "
            f"and <b>{format_date(metric,metric._end_dt,1)}</b>"
        )


def get_grouped_by_str(metric: M.Metric) -> str:
    if metric._group_by:
        grp = metric._group_by._field._name
        return f"grouped by <b>{grp}</b> (top {metric._max_group_count})"
    return ""


def fix_title_text(title_text: str, max_length=MAX_TITLE_LENGTH) -> str:
    if len(title_text) > max_length:
        return title_text[:max_length] + "..."
    else:
        return title_text


def get_segment_title_text(segment: M.Segment) -> str:
    if isinstance(segment, M.SimpleSegment):
        s = cast(M.SimpleSegment, segment)
        if s._operator is None:
            return s._left._event_name
        else:
            left = cast(M.EventFieldDef, s._left)
            right = s._right
            if right is None:
                right = "null"

            return f"{left._event_name} with {left._field._name} {s._operator} {right}"
    elif isinstance(segment, M.ComplexSegment):
        c = cast(M.ComplexSegment, segment)
        return f"{get_segment_title_text(c._left)} {c._operator} {get_segment_title_text(c._right)}"
    else:
        raise ValueError(f"Segment of type {type(segment)} is not supported.")


def get_time_group_text(time_group: M.TimeGroup) -> str:
    if time_group == M.TimeGroup.TOTAL:
        return "total"
    if time_group == M.TimeGroup.DAY:
        return "daily"
    if time_group == M.TimeGroup.MINUTE:
        return "minute by minute"

    return time_group.name.lower() + "ly"


def get_segmentation_title(metric: M.SegmentationMetric):
    if metric._custom_title:
        return metric._custom_title
    segment_str = fix_title_text(get_segment_title_text(metric._segment))
    tg = get_time_group_text(metric._time_group).title()
    lines = [
        f"{tg} count of unique users",
        f"who did <b>{segment_str}</b>",
    ]
    if metric._group_by is not None:
        lines.append(get_grouped_by_str(metric))
    lines.append(get_timeframe_str(metric))
    return "<br />".join(lines)


def get_conversion_title(metric: M.ConversionMetric) -> str:
    if metric._custom_title:
        return metric._custom_title
    events = " then did ".join(
        [
            f"<b>{fix_title_text(get_segment_title_text(seg), 100)}</b>"
            for seg in metric._conversion._segments
        ]
    )

    tg = get_time_group_text(metric._time_group).title()

    if metric._time_group != M.TimeGroup.TOTAL:
        agg = "conversion rate of unique users"
    else:
        agg = "count and cvr. of unique users"

    within_str = f"within {metric._conv_window}"
    group_by = get_grouped_by_str(metric)
    timeframe_str = get_timeframe_str(metric)

    lines = [
        f"{tg} {agg}",
        f"who did {events}",
        f"{within_str}, {group_by}",
        timeframe_str,
    ]

    return "<br />".join(lines)