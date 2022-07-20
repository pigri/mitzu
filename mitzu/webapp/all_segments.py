from __future__ import annotations

from typing import Any, Dict, List

import dash.development.base_component as bc
import mitzu.model as M
import mitzu.webapp.complex_segment as CS
import mitzu.webapp.event_segment as ES
import mitzu.webapp.navbar.metric_type_dropdown as MNB
import mitzu.webapp.simple_segment as SS
import mitzu.webapp.webapp as WA
from dash import html
from dash.dependencies import ALL, Input, Output, State
from mitzu.webapp.helper import deserialize_component, find_components

ALL_SEGMENTS = "all_segments"


class AllSegmentsContainer(html.Div):
    def __init__(
        self, discovered_datasource: M.DiscoveredEventDataSource, metric_type: str
    ):
        container = CS.ComplexSegmentCard(discovered_datasource, 0, metric_type)
        super().__init__(
            id=ALL_SEGMENTS,
            children=[container],
            className=ALL_SEGMENTS,
        )

    @classmethod
    def fix(
        cls,
        complex_seg_children: List[bc.Component],
        discovered_datasource: M.DiscoveredEventDataSource,
        metric_type: str,
    ) -> List[bc.Component]:
        fixed_complex_seg_children = []
        limit = 5

        if metric_type == MNB.SEGMENTATION:
            limit = 1
        elif metric_type == MNB.RETENTION:
            limit = 2

        for i, seg_child in enumerate(complex_seg_children):
            if i >= limit:
                break
            fixed_seg_child = CS.ComplexSegmentCard.fix(
                seg_child, discovered_datasource, i, metric_type
            )
            fixed_complex_seg_children.append(fixed_seg_child)

        res_children: List[CS.ComplexSegmentCard] = []
        for complex_seg in fixed_complex_seg_children:
            event_name_dd = find_components(ES.EVENT_NAME_DROPDOWN, complex_seg)[0]
            event_name_value = event_name_dd.value
            if event_name_value is not None:
                res_children.append(complex_seg)

        if len(res_children) < limit:
            res_children.append(
                CS.ComplexSegmentCard(
                    discovered_datasource, len(res_children), metric_type
                )
            )
        return res_children

    @classmethod
    def get_segments(
        cls,
        all_seg_children: List[bc.Component],
        discovered_datasource: M.DiscoveredEventDataSource,
        metric_type: str,
    ) -> List[M.Segment]:
        res = []
        all_seg_children = cls.fix(all_seg_children, discovered_datasource, metric_type)
        for segment in all_seg_children:
            segment = CS.ComplexSegmentCard.get_segment(segment, discovered_datasource)
            if segment is not None:
                res.append(segment)

        return res

    @classmethod
    def create_callbacks(cls, webapp: WA.MitzuWebApp):
        @webapp.app.callback(
            Output(ALL_SEGMENTS, "children"),
            [
                Input({"type": ES.EVENT_NAME_DROPDOWN, "index": ALL}, "value"),
                Input({"type": SS.PROPERTY_NAME_DROPDOWN, "index": ALL}, "value"),
                Input({"type": SS.PROPERTY_OPERATOR_DROPDOWN, "index": ALL}, "value"),
                Input(MNB.METRIC_TYPE_DROPDOWN, "value"),
                Input(WA.MITZU_LOCATION, "pathname"),
            ],
            State(ALL_SEGMENTS, "children"),
            prevent_initial_call=True,
        )
        def input_changed(
            evt_name_value: Any,
            prop_value: Any,
            op_value: Any,
            metric_type: str,
            pathname: str,
            children: List[Dict],
        ):
            webapp.load_dataset_model(pathname)
            complex_seg_children: List[bc.Component] = [
                deserialize_component(child) for child in children
            ]
            dm = webapp.get_discovered_datasource()
            if dm is None:
                return []

            res_children = cls.fix(complex_seg_children, dm, metric_type)
            return [child.to_plotly_json() for child in res_children]

        SS.SimpleSegmentDiv.create_callbacks(webapp.app)
