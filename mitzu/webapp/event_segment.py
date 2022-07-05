from __future__ import annotations

from typing import Optional
from uuid import uuid4

import mitzu.model as M
from dash import dcc, html
from mitzu.webapp.simple_segment import SimpleSegmentDiv

EVENT_SEGMENT = "event_segment"
EVENT_NAME_DROPDOWN = "event_name_dropdown"
SIMPLE_SEGMENT_CONTAINER = "simple_segment_container"


def creat_event_name_dropdown(
    index, dataset_model: M.DatasetModel, step: int, event_segment_index: int
) -> dcc.Dropdown:
    evt_names = [
        k for k in dataset_model.__class__.__dict__.keys() if not k.startswith("_")
    ]
    evt_names.sort()
    if step == 0 and event_segment_index == 0:
        placeholder = "Select users who did ..."
    elif step > 0 and event_segment_index == 0:
        placeholder = "Then did ..."
    else:
        placeholder = "Or did ..."

    return dcc.Dropdown(
        options=evt_names,
        value=None,
        multi=False,
        className=EVENT_NAME_DROPDOWN,
        placeholder=placeholder,
        id={
            "type": EVENT_NAME_DROPDOWN,
            "index": index,
        },
    )


def create_container(index: str) -> html.Div:
    return html.Div(id={"type": SIMPLE_SEGMENT_CONTAINER, "index": index}, children=[])


class EventSegmentDiv(html.Div):
    def __init__(
        self,
        dataset_model: M.DatasetModel,
        step: int,
        event_segment_index: int,
    ):
        index = str(uuid4())
        event_dd = creat_event_name_dropdown(
            index, dataset_model, step, event_segment_index
        )
        container = create_container(index)
        super().__init__(
            id={"type": EVENT_SEGMENT, "index": index},
            children=[event_dd, container],
            className=EVENT_SEGMENT,
        )

    @classmethod
    def fix(cls, event_segment: html.Div, dataset_model: M.DatasetModel) -> html.Div:
        children = event_segment.children
        evt_name_dd = children[0]
        props = children[1]

        if evt_name_dd.value is None:
            props.children = []
        else:
            res_props_children = []
            for prop in props.children:
                if prop.children[0].value is not None:
                    prop = SimpleSegmentDiv.fix(prop, dataset_model)
                    res_props_children.append(prop)
            res_props_children.append(
                SimpleSegmentDiv(
                    evt_name_dd.value, dataset_model, len(res_props_children)
                )
            )
            props.children = res_props_children

        return event_segment

    @classmethod
    def get_segment(
        cls, event_segment: html.Div, dataset_model: M.DatasetModel
    ) -> Optional[M.Segment]:
        children = event_segment.children
        event_name = children[0].value
        simple_seg_children = children[1].children
        if (
            len(simple_seg_children) == 1
            and simple_seg_children[0].children[0].value is None
        ):
            return dataset_model.__class__.__dict__[event_name]

        res_segment = None
        for seg_child in simple_seg_children:
            simple_seg = SimpleSegmentDiv.get_simple_segment(seg_child, dataset_model)
            if simple_seg is None:
                continue
            if res_segment is None:
                res_segment = simple_seg
            else:
                res_segment = res_segment & simple_seg

        return res_segment
