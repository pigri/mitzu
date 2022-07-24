from __future__ import annotations

from typing import List, Optional, Union

import dash.development.base_component as bc
import mitzu.model as M
import mitzu.webapp.all_segments as AS
import mitzu.webapp.complex_segment as CS
import mitzu.webapp.dates_selector as DS
import mitzu.webapp.metrics_config as MC
from mitzu.webapp.complex_segment import ComplexSegmentCard
from mitzu.webapp.helper import (
    find_components,
    find_event_field_def,
    find_first_component,
)


def create_metric(
    complex_segs: List[ComplexSegmentCard],
    mc_children: List[bc.Component],
    discovered_datasource: M.DiscoveredEventDataSource,
    metric_type: str,
) -> Optional[M.Metric]:
    complex_segments = AS.AllSegmentsContainer.get_complex_segments(
        complex_segs, discovered_datasource
    )
    metric: Optional[Union[M.Segment, M.Conversion]] = None
    for seg in complex_segments:
        if metric is None:
            metric = seg
        else:
            metric = metric >> seg
    if metric is None:
        return None

    conv_window_interval = find_first_component(
        MC.CONVERSION_WINDOW_INTERVAL, mc_children
    ).value
    conv_window_interval_steps = find_first_component(
        MC.CONVERSION_WINDOW_INTERVAL_STEPS, mc_children
    ).value

    date_selector = find_first_component(DS.DATE_SELECTOR, mc_children)
    time_group = DS.get_metric_timegroup(date_selector)
    lookback_days = DS.get_metric_lookback_days(date_selector)
    start_date, end_date = None, None
    if lookback_days is None:
        start_date, end_date = DS.get_metric_custom_dates(date_selector)

    group_by_path = find_components(CS.COMPLEX_SEGMENT_GROUP_BY, complex_segs[0])[
        0
    ].value
    group_by = None
    if group_by_path is not None:
        group_by = find_event_field_def(group_by_path, discovered_datasource)

    if len(complex_segments) > 1 and isinstance(metric, M.Conversion):
        conv_window = M.TimeWindow(
            conv_window_interval, M.TimeGroup(conv_window_interval_steps)
        )
        return metric.config(
            time_group=M.TimeGroup(time_group),
            conv_window=conv_window,
            group_by=group_by,
            lookback_days=lookback_days,
            start_dt=start_date,
            end_dt=end_date,
            custom_title="",
        )
    elif isinstance(metric, M.Segment):
        return metric.config(
            time_group=M.TimeGroup(time_group),
            group_by=group_by,
            lookback_days=lookback_days,
            start_dt=start_date,
            end_dt=end_date,
            custom_title="",
        )
    raise Exception("Invalid metric type")
