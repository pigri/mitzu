from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import mitzu.model as M
import mitzu.webapp.complex_segment_handler as CS
import mitzu.webapp.navbar.metric_type_handler as MNB
from dash import html

METRIC_SEGMENTS = "metric_segments"


@dataclass
class MetricSegmentsHandler:

    discovered_project: M.DiscoveredProject
    component: html.Div

    @classmethod
    def from_metric(
        cls,
        discovered_project: M.DiscoveredProject,
        metric: Optional[M.Metric],
        metric_type: MNB.MetricType,
    ) -> MetricSegmentsHandler:
        segments = []
        if isinstance(metric, M.SegmentationMetric):
            limit = 1
            segments = [metric._segment]
        elif isinstance(metric, M.ConversionMetric):
            limit = 10
            segments = metric._conversion._segments
        elif isinstance(metric, M.RetentionMetric):
            limit = 2
            segments = metric._conversion._segments
        elif metric is None:
            limit = 1
            segments = []

        fixed_metric_comps = []
        for funnel_step, segment in enumerate(segments):
            csh = CS.ComplexSegmentHandler.from_segment(
                funnel_step=funnel_step,
                segment=segment,
                discovered_project=discovered_project,
                metric=metric,
                metric_type=metric_type,
            )
            fixed_metric_comps.append(csh.component)

        if len(fixed_metric_comps) < limit:
            fixed_metric_comps.append(
                CS.ComplexSegmentHandler.from_segment(
                    discovered_project,
                    len(fixed_metric_comps),
                    None,
                    None,
                    metric_type,
                ).component
            )

        return MetricSegmentsHandler(
            component=html.Div(
                id=METRIC_SEGMENTS,
                children=fixed_metric_comps,
                className=METRIC_SEGMENTS,
            ),
            discovered_project=discovered_project,
        )

    def to_metric_segments(self) -> List[M.Segment]:
        res = []
        for complex_seg_comp in self.component.children:
            csh = CS.ComplexSegmentHandler.from_component(
                complex_seg_comp, self.discovered_project
            )
            complex_segment = csh.to_segment()
            if complex_segment is not None:
                res.append(complex_segment)
        return res

    @classmethod
    def from_component(
        cls, discovered_project: M.DiscoveredProject, component: html.Div
    ) -> MetricSegmentsHandler:
        return MetricSegmentsHandler(
            discovered_project=discovered_project, component=component
        )
