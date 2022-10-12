from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import mitzu.model as M
import mitzu.webapp.simple_segment_handler as SS
from dash import dcc, html
from mitzu.webapp.helper import find_first_component, get_event_names, value_to_label

EVENT_SEGMENT = "event_segment"
EVENT_NAME_DROPDOWN = "event_name_dropdown"
SIMPLE_SEGMENT_CONTAINER = "simple_segment_container"


def create_event_name_dropdown(
    index,
    discovered_project: M.DiscoveredProject,
    step: int,
    event_segment_index: int,
    segment: Optional[M.Segment],
) -> dcc.Dropdown:
    options = (
        [
            {"label": value_to_label(v), "value": v}
            for v in discovered_project.get_all_events()
        ]
        if discovered_project is not None
        else []
    )
    options.sort(key=lambda v: v["label"])

    if step == 0 and event_segment_index == 0:
        placeholder = "+ Select Event"
    elif step > 0 and event_segment_index == 0:
        placeholder = "+ Then"
    else:
        placeholder = "+ Select Another Event"

    segment_event_names = get_event_names(segment)
    if len(set(segment_event_names)) > 1:
        raise Exception("Multiple event complex segment for EventSegmentHandler")
    value = None if len(segment_event_names) == 0 else segment_event_names[0]

    return dcc.Dropdown(
        options=options,
        value=value,
        multi=False,
        className=EVENT_NAME_DROPDOWN,
        placeholder=placeholder,
        id={
            "type": EVENT_NAME_DROPDOWN,
            "index": index,
        },
    )


def get_event_simple_segments(segment: M.Segment) -> List[M.SimpleSegment]:
    if isinstance(segment, M.SimpleSegment):
        return [segment]
    elif isinstance(segment, M.ComplexSegment):
        if segment._operator != M.BinaryOperator.AND:
            raise Exception(
                f"Invalid event level complex segment operator: {segment._operator}"
            )
        return get_event_simple_segments(segment._left) + get_event_simple_segments(
            segment._right
        )
    else:
        raise Exception(f"Unsupported Segment Type: {type(segment)}")


def create_simple_segments_container(
    type_index: str,
    segment: Optional[M.Segment],
    discovered_project: M.DiscoveredProject,
) -> Optional[html.Div]:
    children = []
    if segment is None:
        return None
    event_simple_segments = get_event_simple_segments(segment)

    if isinstance(segment, M.ComplexSegment):
        for seg_index, ess in enumerate(event_simple_segments):
            comp = SS.SimpleSegmentHandler.from_simple_segment(
                ess, discovered_project, type_index, seg_index
            ).component
            children.append(comp)
    elif isinstance(segment, M.SimpleSegment) and isinstance(
        segment._left, M.EventFieldDef
    ):
        comp = SS.SimpleSegmentHandler.from_simple_segment(
            segment, discovered_project, type_index, 0
        ).component
        children.append(comp)

    event_name = event_simple_segments[0]._left._event_name
    event_def = discovered_project.get_event_def(event_name)
    if event_def is None:
        raise Exception(
            f"Invalid state, {event_name} is not possible to find in discovered datasource."
        )
    children.append(
        SS.SimpleSegmentHandler.from_simple_segment(
            M.SimpleSegment(event_def),
            discovered_project,
            type_index,
            len(children),
        ).component
    )

    return html.Div(
        id={"type": SIMPLE_SEGMENT_CONTAINER, "index": type_index}, children=children
    )


@dataclass
class EventSegmentHandler:

    discovered_project: M.DiscoveredProject
    component: html.Div

    @classmethod
    def from_component(
        cls, component: html.Div, discovered_project: M.DiscoveredProject
    ):
        return EventSegmentHandler(
            component=component, discovered_project=discovered_project
        )

    @classmethod
    def from_segment(
        cls,
        segment: Optional[M.Segment],
        discovered_project: M.DiscoveredProject,
        funnel_step: int,
        event_segment_index: int,
    ) -> EventSegmentHandler:
        type_index = f"{funnel_step}-{event_segment_index}"
        event_dd = create_event_name_dropdown(
            type_index, discovered_project, funnel_step, event_segment_index, segment
        )
        children = [event_dd]
        simples_segs_container = create_simple_segments_container(
            type_index, segment, discovered_project
        )
        if simples_segs_container is not None:
            children.append(simples_segs_container)

        component = html.Div(
            id={"type": EVENT_SEGMENT, "index": type_index},
            children=children,
            className=EVENT_SEGMENT,
        )
        return EventSegmentHandler(
            discovered_project=discovered_project, component=component
        )

    def to_segment(self) -> Optional[M.Segment]:
        event_name_dd = find_first_component(
            EVENT_NAME_DROPDOWN, self.component.children
        )
        if event_name_dd.value is None:
            return None
        ssc = find_first_component(SIMPLE_SEGMENT_CONTAINER, self.component)

        if ssc is None:
            if event_name_dd.value is not None:
                return M.SimpleSegment(
                    _left=self.discovered_project.get_event_def(event_name_dd.value)
                )
            else:
                return None

        ssc_children = ssc.children
        if len(ssc_children) == 1 and ssc_children[0].children[0].value is None:
            return M.SimpleSegment(
                _left=self.discovered_project.get_event_def(event_name_dd.value)
            )

        res_segment = None
        for seg_child in ssc_children:
            simple_seg_handler = SS.SimpleSegmentHandler.from_component(
                component=seg_child, discovered_project=self.discovered_project
            )
            simple_seg = simple_seg_handler.to_simple_segment()

            if simple_seg is None:
                continue
            if simple_seg._left._event_name != event_name_dd.value:
                continue

            if res_segment is None:
                res_segment = simple_seg
            else:
                res_segment = res_segment & simple_seg
        if res_segment is None and event_name_dd.value is not None:
            return M.SimpleSegment(
                _left=self.discovered_project.get_event_def(event_name_dd.value)
            )
        return res_segment
