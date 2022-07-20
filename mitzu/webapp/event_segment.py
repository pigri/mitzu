from __future__ import annotations

from typing import Optional
from uuid import uuid4

import mitzu.model as M
from dash import dcc, html
from mitzu.webapp.helper import find_first_component, value_to_label
from mitzu.webapp.simple_segment import SimpleSegmentDiv

EVENT_SEGMENT = "event_segment"
EVENT_NAME_DROPDOWN = "event_name_dropdown"
SIMPLE_SEGMENT_CONTAINER = "simple_segment_container"


def creat_event_name_dropdown(
    index,
    discovered_datasource: M.DiscoveredEventDataSource,
    step: int,
    event_segment_index: int,
) -> dcc.Dropdown:
    evt_names = (
        [
            {"label": value_to_label(v), "value": v}
            for v in discovered_datasource.get_all_events()
        ]
        if discovered_datasource is not None
        else []
    )
    evt_names.sort(key=lambda v: v["label"])
    if step == 0 and event_segment_index == 0:
        placeholder = "+ Select Event"
    elif step > 0 and event_segment_index == 0:
        placeholder = "+ Then"
    else:
        placeholder = "+ Or Event"

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
        discovered_datasource: M.DiscoveredEventDataSource,
        step: int,
        event_segment_index: int,
    ):
        index = str(uuid4())
        event_dd = creat_event_name_dropdown(
            index, discovered_datasource, step, event_segment_index
        )
        container = create_container(index)
        super().__init__(
            id={"type": EVENT_SEGMENT, "index": index},
            children=[event_dd, container],
            className=EVENT_SEGMENT,
        )

    @classmethod
    def fix(
        cls,
        event_segment: html.Div,
        discovered_datasource: M.DiscoveredEventDataSource,
    ) -> html.Div:
        children = event_segment.children
        evt_name_dd = children[0]
        props = children[1]

        if evt_name_dd.value is None:
            props.children = []
        else:
            res_props_children = []
            for prop in props.children:
                if prop.children[0].value is not None:
                    prop = SimpleSegmentDiv.fix(prop, discovered_datasource)
                    res_props_children.append(prop)
            res_props_children.append(
                SimpleSegmentDiv(
                    evt_name_dd.value, discovered_datasource, len(res_props_children)
                )
            )
            props.children = res_props_children

        return event_segment

    @classmethod
    def get_segment(
        cls,
        event_segment: html.Div,
        discovered_datasource: M.DiscoveredEventDataSource,
    ) -> Optional[M.Segment]:

        event_name_dd = find_first_component(
            EVENT_NAME_DROPDOWN, event_segment.children
        )
        ssc_children = find_first_component(
            SIMPLE_SEGMENT_CONTAINER, event_segment
        ).children

        if len(ssc_children) == 1 and ssc_children[0].children[0].value is None:
            return M.SimpleSegment(
                _left=discovered_datasource.get_all_events()[event_name_dd.value]
            )

        res_segment = None
        for seg_child in ssc_children:
            simple_seg = SimpleSegmentDiv.get_simple_segment(
                seg_child, discovered_datasource
            )
            if simple_seg is None:
                continue
            if res_segment is None:
                res_segment = simple_seg
            else:
                res_segment = res_segment & simple_seg

        return res_segment
