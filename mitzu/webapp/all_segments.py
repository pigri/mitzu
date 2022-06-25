from __future__ import annotations

from typing import Any, Dict, List

import dash.development.base_component as bc
import mitzu.model as M
from dash import Dash, html
from dash.dependencies import ALL, Input, Output, State
from mitzu.webapp.complex_segment import ComplexSegment
from mitzu.webapp.event_segment import EVENT_NAME_DROPDOWN
from mitzu.webapp.helper import deserialize_component
from mitzu.webapp.simple_segment import (
    PROPERTY_NAME_DROPDOWN,
    PROPERTY_OPERATOR_DROPDOWN,
    SimpleSegment,
)

ALL_SEGMENTS = "all_segments"


class AllSegmentsContainer(html.Div):
    def __init__(self, dataset_model: M.DatasetModel):
        container = ComplexSegment(dataset_model, 0)
        super().__init__(
            id=ALL_SEGMENTS,
            children=[container],
            className=ALL_SEGMENTS,
        )

    @classmethod
    def fix(
        cls, complex_seg_children: List[bc.Component], dataset_model: M.DatasetModel
    ) -> List[bc.Component]:

        fixed_complex_seg_children = []
        for i, seg_child in enumerate(complex_seg_children):
            fixed_seg_child = ComplexSegment.fix(seg_child, dataset_model, i)
            fixed_complex_seg_children.append(fixed_seg_child)

        res_children: List[ComplexSegment] = []
        for complex_seg in fixed_complex_seg_children:
            event_name_value = complex_seg.children[1].children[0].children[0].value
            if event_name_value is not None:
                res_children.append(complex_seg)
        res_children.append(ComplexSegment(dataset_model, len(res_children)))
        return res_children

    @classmethod
    def get_segments(
        cls, all_seg_children: List[bc.Component], dataset_model: M.DatasetModel
    ) -> List[M.Segment]:
        res = []
        all_seg_children = cls.fix(all_seg_children, dataset_model)
        for segment in all_seg_children:

            segment = ComplexSegment.get_segment(segment, dataset_model)
            if segment is not None:
                res.append(segment)

        return res

    @classmethod
    def create_callbacks(
        cls,
        app: Dash,
        dataset_model: M.DatasetModel,
    ):
        @app.callback(
            Output(ALL_SEGMENTS, "children"),
            [
                Input({"type": EVENT_NAME_DROPDOWN, "index": ALL}, "value"),
                Input({"type": PROPERTY_NAME_DROPDOWN, "index": ALL}, "value"),
                Input({"type": PROPERTY_OPERATOR_DROPDOWN, "index": ALL}, "value"),
            ],
            State(ALL_SEGMENTS, "children"),
            prevent_initial_call=True,
        )
        def input_changed(
            evt_name_value: Any, prop_value: Any, op_value: Any, children: List[Dict]
        ):
            complex_seg_children: List[bc.Component] = [
                deserialize_component(child) for child in children
            ]
            res_children = cls.fix(complex_seg_children, dataset_model)

            return [child.to_plotly_json() for child in res_children]

        SimpleSegment.create_callbacks(app)
