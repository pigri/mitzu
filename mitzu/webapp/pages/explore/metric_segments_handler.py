from __future__ import annotations

from typing import Any, Dict, List, Optional

import dash.development.base_component as bc
import mitzu.model as M
import mitzu.webapp.pages.explore.complex_segment_handler as CS
import mitzu.webapp.pages.explore.metric_type_handler as MNB
from dash import html
from mitzu.webapp.helper import CHILDREN, METRIC_SEGMENTS


def from_metric(
    metric: Optional[M.Metric],
    discovered_project: M.DiscoveredProject,
) -> bc.Component:
    segments = []
    if isinstance(metric, M.SegmentationMetric):
        limit = 1
        segments = [metric._segment]
        metric_type = MNB.MetricType.SEGMENTATION
    elif isinstance(metric, M.ConversionMetric):
        limit = 10
        segments = metric._conversion._segments
        metric_type = MNB.MetricType.CONVERSION
    elif isinstance(metric, M.RetentionMetric):
        limit = 2
        segments = [metric._initial_segment, metric._retaining_segment]
        metric_type = MNB.MetricType.RETENTION
    elif metric is None:
        limit = 1
        segments = []
        metric_type = MNB.MetricType.SEGMENTATION

    fixed_metric_comps = []
    for funnel_step, segment in enumerate(segments):
        fixed_metric_comps.append(
            CS.from_segment(
                funnel_step=funnel_step,
                segment=segment,
                discovered_project=discovered_project,
                metric=metric,
                metric_type=metric_type,
            )
        )

    if len(fixed_metric_comps) < limit:
        fixed_metric_comps.append(
            CS.from_segment(
                discovered_project,
                len(fixed_metric_comps),
                None,
                None,
                metric_type,
            )
        )

    return html.Div(
        id=METRIC_SEGMENTS,
        children=fixed_metric_comps,
        className=METRIC_SEGMENTS,
    )


def from_all_inputs(
    discovered_project: Optional[M.DiscoveredProject],
    all_inputs: Dict[str, Any],
) -> List[M.Segment]:
    res: List[M.Segment] = []
    if discovered_project is None:
        return res
    complex_segments = all_inputs.get(METRIC_SEGMENTS, {}).get(CHILDREN, {})
    for _, complex_segment in complex_segments.items():
        csh = CS.from_all_inputs(discovered_project, complex_segment)
        if csh is not None:
            res.append(csh)
    return res
