from __future__ import annotations

from typing import Dict, List, Optional
from uuid import uuid4

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.event_segment as ES
from dash import dcc, html
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
            field_name = value_to_label(field._get_name()).split(".")[-1]
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
        group_by = html.Div(
            [create_group_by_dropdown(index, None, [], discovered_datasource)],
            className=COMPLEX_SEGMENT_FOOTER,
        )
        header = dbc.CardHeader(
            "Events" if metric_type == "segmentation" else f"{step+1}. Step",
            style={"font-size": "14px", "padding": "6px", "font-weight": "bold"},
        )
        body = html.Div(
            children=[ES.EventSegmentDiv(discovered_datasource, step, 0)],
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
        res_segment = None
        event_segment_divs = find_components(ES.EVENT_SEGMENT, complex_segment)
        for event_segment_div in event_segment_divs:
            event_segment = ES.EventSegmentDiv.get_segment(
                event_segment_div, discovered_datasource
            )
            if event_segment is None:
                continue
            if res_segment is None:
                res_segment = event_segment
            else:
                res_segment = res_segment | event_segment

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
        res_props_children = []
        event_segments = find_components(ES.EVENT_SEGMENT, complex_segment)

        for event_segment in event_segments:
            if event_segment.children[0].value is not None:
                prop = ES.EventSegmentDiv.fix(event_segment, discovered_datasource)
                res_props_children.append(prop)
        res_props_children.append(
            ES.EventSegmentDiv(discovered_datasource, step, len(res_props_children))
        )

        cls.fix_group_by_dd(complex_segment, res_props_children, discovered_datasource)
        complex_segment.children[0].children = (
            "Events" if metric_type == "segmentation" else f"{step+1}. Step"
        )
        complex_segment.children[1].children = res_props_children
        return complex_segment
