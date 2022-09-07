from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.event_segment_handler as ES
import mitzu.webapp.navbar.metric_type_handler as MNB
from dash import dcc, html
from mitzu.webapp.helper import find_components, get_event_names, get_property_name_comp

COMPLEX_SEGMENT = "complex_segment"
COMPLEX_SEGMENT_BODY = "complex_segment_body"
COMPLEX_SEGMENT_FOOTER = "complex_segment_footer"
COMPLEX_SEGMENT_GROUP_BY = "complex_segment_group_by"


def get_group_by_options(
    discovered_project: M.DiscoveredProject, event_names: List[str]
):
    options: List[Dict[str, str]] = []
    for event_name in event_names:
        for field in discovered_project.get_event_def(event_name)._fields:
            field_value = field._get_name()
            should_break = False
            final_field_value = f"{event_name}.{field_value}"
            for op in options:
                if ".".join(op["value"].split(".")[1:]) == field_value:
                    should_break = True
                    break
            if not should_break:
                options.append(
                    {
                        "label": get_property_name_comp(field._get_name()),
                        "value": final_field_value,
                    }
                )
    options.sort(key=lambda v: ".".join(v["value"].split(".")[1:]))
    return options


def create_group_by_dropdown(
    index: str,
    metric: Optional[M.Metric],
    segment: M.Segment,
    discovered_project: M.DiscoveredProject,
) -> dcc:
    event_names = get_event_names(segment)
    group_by_efd = metric._config.group_by if metric is not None else None

    value = None
    if group_by_efd is not None:
        value = f"{group_by_efd._event_name}.{group_by_efd._field._get_name()}"

    options = get_group_by_options(discovered_project, event_names)
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
        placeholder="+ Break Down",
        style={"width": "100%"},
    )


def find_all_event_segments(segment: M.Segment) -> List[M.Segment]:
    if segment is None:
        return []
    if isinstance(segment, M.SimpleSegment):
        return [segment]
    if isinstance(segment, M.ComplexSegment):
        if segment._operator == M.BinaryOperator.OR:
            return find_all_event_segments(segment._left) + find_all_event_segments(
                segment._right
            )
        else:
            return [segment]
    return []


@dataclass
class ComplexSegmentHandler:

    discovered_project: M.DiscoveredProject
    component: dbc.Card

    @classmethod
    def from_segment(
        self,
        discovered_project: M.DiscoveredProject,
        funnel_step: int,
        metric: Optional[M.Metric],
        segment: Optional[M.Segment],
        metric_type: MNB.MetricType,
    ) -> ComplexSegmentHandler:
        type_index = str(funnel_step)
        header = dbc.CardHeader(
            "Events"
            if metric_type == MNB.MetricType.SEGMENTATION
            else f"{funnel_step+1}. Step",
            style={"font-size": "14px", "padding": "6px", "font-weight": "bold"},
        )

        body_children = []

        if segment is not None:
            event_segments = find_all_event_segments(segment)
            for index, evt_segment in enumerate(event_segments):
                body_children.append(
                    ES.EventSegmentHandler.from_segment(
                        evt_segment, discovered_project, funnel_step, index
                    ).component
                )
        body_children.append(
            ES.EventSegmentHandler.from_segment(
                None, discovered_project, funnel_step, len(body_children)
            ).component
        )

        if segment is not None and funnel_step == 0:
            group_by_dd = html.Div(
                [
                    create_group_by_dropdown(
                        type_index, metric, segment, discovered_project
                    )
                ],
                className=COMPLEX_SEGMENT_FOOTER,
            )
            body_children.append(group_by_dd)

        card_body = dbc.CardBody(
            children=body_children,
            className=COMPLEX_SEGMENT_BODY,
        )
        return ComplexSegmentHandler(
            discovered_project=discovered_project,
            component=dbc.Card(
                id={"type": COMPLEX_SEGMENT, "index": type_index},
                children=[header, card_body],
                className=COMPLEX_SEGMENT,
            ),
        )

    def to_segment(self) -> Optional[M.Segment]:
        res_segment = None
        event_segment_divs = find_components(ES.EVENT_SEGMENT, self.component)
        for event_segment_div in event_segment_divs:
            esh = ES.EventSegmentHandler.from_component(
                event_segment_div, self.discovered_project
            )
            event_segment = esh.to_segment()
            if event_segment is None:
                continue
            if res_segment is None:
                res_segment = event_segment
            else:
                res_segment = res_segment | event_segment

        return res_segment

    @classmethod
    def from_component(
        cls, component: dbc.Card, discovered_project: M.DiscoveredProject
    ) -> ComplexSegmentHandler:
        return ComplexSegmentHandler(
            discovered_project=discovered_project, component=component
        )
