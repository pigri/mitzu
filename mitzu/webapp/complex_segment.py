from __future__ import annotations

from typing import Dict, List, Optional
from uuid import uuid4

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.navbar.metric_type_dropdown as MNB
from dash import dcc, html
from mitzu.webapp.event_segment import EventSegmentDiv
from mitzu.webapp.helper import find_components, value_to_label

COMPLEX_SEGMENT = "complex_segment"
COMPLEX_SEGMENT_BODY = "complex_segment_body"
COMPLEX_SEGMENT_FOOTER = "complex_segment_footer"
COMPLEX_SEGMENT_GROUP_BY = "complex_segment_group_by"


def get_group_by_options(
    discovered_datasource: M.DiscoveredEventDataSource, event_names: List[str]
):
    options: List[Dict[str, str]] = []
    events = (
        discovered_datasource.get_all_events()
        if discovered_datasource is not None
        else {}
    )
    for event_name in event_names:
        for field in events[event_name]._fields:
            field_name = value_to_label(field._get_name()).replace(".", "/")
            field_value = field._get_name()
            should_break = False
            for op in options:
                if op["label"] == field_name:
                    should_break = True
                    break
            if not should_break:
                options.append(
                    {"label": field_name, "value": f"{event_name}.{field_value}"}
                )
    options.sort(key=lambda v: v["label"])
    return options


def create_group_by_dropdown(
    index: str,
    value: Optional[str],
    event_names: List[str],
    discovered_datasource: M.DiscoveredEventDataSource,
) -> dcc:
    options = get_group_by_options(discovered_datasource, event_names)
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
        placeholder="- Group By",
        style={"width": "100%"},
    )


class ComplexSegmentCard(dbc.Card):
    def __init__(
        self,
        discovered_datasource: M.DiscoveredEventDataSource,
        step: int,
        metric_type: str,
    ):
        index = str(uuid4())
        header = dbc.CardHeader(
            [html.B("Events" if metric_type == MNB.SEGMENTATION else f"Step {step+1}.")]
        )
        group_by = html.Div(
            [create_group_by_dropdown(index, None, [], discovered_datasource)],
            className=COMPLEX_SEGMENT_FOOTER,
            style={"padding": "4px"},
        )
        body = dbc.CardBody(
            children=[EventSegmentDiv(discovered_datasource, step, 0)],
            className=COMPLEX_SEGMENT_BODY,
        )
        super().__init__(
            id={"type": COMPLEX_SEGMENT, "index": index},
            children=[header, body, group_by],
            className=COMPLEX_SEGMENT,
        )

    @classmethod
    def get_segment(
        cls,
        complex_segment: dbc.Card,
        discovered_datasource: M.DiscoveredEventDataSource,
    ) -> Optional[M.Segment]:
        children = complex_segment.children[1].children
        res_segment = None
        for seg_child in children:
            complex_segment = EventSegmentDiv.get_segment(
                seg_child, discovered_datasource
            )
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
        discovered_datasource: M.DiscoveredEventDataSource,
    ) -> None:
        group_by = find_components(COMPLEX_SEGMENT_GROUP_BY, complex_segment)[0]
        event_names = []
        for evt_seg in res_props_children:
            if evt_seg.children[0].value is not None:
                event_names.append(evt_seg.children[0].value)

        options = get_group_by_options(discovered_datasource, event_names)
        group_by.options = options

    @classmethod
    def fix(
        cls,
        complex_segment: dbc.Card,
        discovered_datasource: M.DiscoveredEventDataSource,
        step: int,
        metric_type: str,
    ) -> ComplexSegmentCard:
        complex_segment.children[0].children[0].children = (
            "Events" if metric_type == MNB.SEGMENTATION else f"Step {step+1}."
        )
        res_props_children = []
        for event_segment in complex_segment.children[1].children:
            if event_segment.children[0].value is not None:
                prop = EventSegmentDiv.fix(event_segment, discovered_datasource)
                res_props_children.append(prop)
        res_props_children.append(
            EventSegmentDiv(discovered_datasource, step, len(res_props_children))
        )

        cls.fix_group_by_dd(complex_segment, res_props_children, discovered_datasource)

        complex_segment.children[1].children = res_props_children
        return complex_segment
