from __future__ import annotations

from typing import Dict, List, Optional
from uuid import uuid4

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.webapp as WA
from dash import dcc, html
from mitzu.webapp.event_segment import EventSegmentDiv
from mitzu.webapp.helper import recursive_find_all_props

COMPLEX_SEGMENT = "complex_segment"
COMPLEX_SEGMENT_BODY = "complex_segment_body"
COMPLEX_SEGMENT_FOOTER = "complex_segment_footer"
COMPLEX_SEGMENT_GROUP_BY = "complex_segment_group_by"


def create_group_by_dropdown(
    index: str,
    value: Optional[str],
    event_names: List[str],
    dataset_model: M.DatasetModel,
) -> dcc:
    options: List[Dict[str, str]] = []
    for event_name in event_names:
        vals = recursive_find_all_props(dataset_model.__class__.__dict__[event_name])
        for v in vals:
            should_break = False
            for op in options:
                if op["label"] == v:
                    should_break = True
                    break
            if not should_break:
                options.append({"label": v, "value": f"{event_name}.{v}"})
    options.sort(key=lambda v: v["label"])

    if value not in [v["value"] for v in options]:
        value = None

    return dcc.Dropdown(
        id={"type": COMPLEX_SEGMENT_GROUP_BY, "index": index},
        options=options,
        value=value,
        clearable=True,
        searchable=True,
        multi=False,
        className=COMPLEX_SEGMENT_GROUP_BY,
        placeholder="Select property",
    )


class ComplexSegmentCard(dbc.Card):
    def __init__(self, dataset_model: M.DatasetModel, step: int, metric_type: str):
        index = str(uuid4())
        header = dbc.CardHeader(
            children=[
                html.B(
                    "Events" if metric_type == WA.SEGMENTATION else f"Step {step+1}."
                )
            ]
        )
        footer = dbc.CardFooter(
            className=COMPLEX_SEGMENT_FOOTER,
            children=[
                dbc.Row(
                    children=[
                        dbc.Col(html.B("Group by"), width=3),
                        dbc.Col(
                            create_group_by_dropdown(index, None, [], dataset_model),
                            width=9,
                        ),
                    ],
                    align="center",
                    justify="start",
                )
            ],
        )
        body = dbc.CardBody(
            children=[EventSegmentDiv(dataset_model, step, 0)],
            className=COMPLEX_SEGMENT_BODY,
        )
        super().__init__(
            id={"type": COMPLEX_SEGMENT, "index": index},
            children=[header, body, footer],
            className=COMPLEX_SEGMENT,
        )

    @classmethod
    def get_segment(
        cls, complex_segment: dbc.Card, dataset_model: M.DatasetModel
    ) -> Optional[M.Segment]:
        children = complex_segment.children[1].children
        res_segment = None
        for seg_child in children:
            complex_segment = EventSegmentDiv.get_segment(seg_child, dataset_model)
            if complex_segment is None:
                continue
            if res_segment is None:
                res_segment = complex_segment
            else:
                res_segment = res_segment | complex_segment

        return res_segment

    def fix_group_by_dd(
        complex_segment: dbc.Card,
        res_props_children: List[bc.Component],
        dataset_model: M.DatasetModel,
    ) -> None:
        group_by = complex_segment.children[2].children[0].children[1].children[0]
        event_names = []
        for evt_seg in res_props_children:
            if evt_seg.children[0].value is not None:
                event_names.append(evt_seg.children[0].value)

        new_group_by_drop_down = create_group_by_dropdown(
            complex_segment.id["index"], group_by.value, event_names, dataset_model
        )

        if new_group_by_drop_down.options != group_by.options:
            complex_segment.children[2].children[0].children[1].children[
                0
            ] = new_group_by_drop_down

    @classmethod
    def fix(
        cls,
        complex_segment: dbc.Card,
        dataset_model: M.DatasetModel,
        step: int,
        metric_type: str,
    ) -> ComplexSegmentCard:
        complex_segment.children[0].children[0].children = (
            "Events" if metric_type == WA.SEGMENTATION else f"Step {step+1}."
        )
        res_props_children = []
        for event_segment in complex_segment.children[1].children:
            if event_segment.children[0].value is not None:
                prop = EventSegmentDiv.fix(event_segment, dataset_model)
                res_props_children.append(prop)
        res_props_children.append(
            EventSegmentDiv(dataset_model, step, len(res_props_children))
        )

        cls.fix_group_by_dd(complex_segment, res_props_children, dataset_model)

        complex_segment.children[1].children = res_props_children
        return complex_segment
