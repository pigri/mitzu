from __future__ import annotations

from typing import Any, Dict, List, Optional

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.pages.explore.event_segment_handler as ES
import mitzu.webapp.pages.explore.metric_type_handler as MTH
from dash import html
from mitzu.webapp.helper import (
    CHILDREN,
    get_event_names,
    get_property_name_label,
    WITH_VALUE_CLS,
)
import dash_mantine_components as dmc

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
                        "label": get_property_name_label(field._get_name()),
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
) -> dmc.Select:
    event_names = get_event_names(segment)
    group_by_efd = metric._config.group_by if metric is not None else None

    value = None
    if group_by_efd is not None:
        value = f"{group_by_efd._event_name}.{group_by_efd._field._get_name()}"

    options = get_group_by_options(discovered_project, event_names)
    if value not in [v["value"] for v in options]:
        value = None

    return dmc.Select(
        id={"type": COMPLEX_SEGMENT_GROUP_BY, "index": index},
        data=options,
        value=value,
        clearable=True,
        searchable=True,
        className=(
            COMPLEX_SEGMENT_GROUP_BY
            + " "
            + ("" if value is None else WITH_VALUE_CLS)
            + " border border-0 border-top-1 m-2 rounded-2"
        ),
        placeholder="+ Break Down",
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


def from_segment(
    discovered_project: M.DiscoveredProject,
    funnel_step: int,
    metric: Optional[M.Metric],
    segment: Optional[M.Segment],
    metric_type: MTH.MetricType,
) -> bc.Component:
    type_index = str(funnel_step)
    if metric_type == MTH.MetricType.CONVERSION:
        title = f"{funnel_step+1}. Step"
    elif metric_type == MTH.MetricType.RETENTION:
        title = "Initial Step" if funnel_step == 0 else "Retaining Step"
    else:
        title = "Events"

    header = dbc.CardHeader(title, class_name="p-2 fw-bold")

    body_children = []

    if segment is not None:
        event_segments = find_all_event_segments(segment)
        for index, evt_segment in enumerate(event_segments):
            body_children.append(
                ES.from_segment(evt_segment, discovered_project, funnel_step, index)
            )
    body_children.append(
        ES.from_segment(None, discovered_project, funnel_step, len(body_children))
    )

    if (
        segment is not None
        and funnel_step == 0
        and not (
            isinstance(metric, M.RetentionMetric)
            and metric._time_group != M.TimeGroup.TOTAL
        )
    ):
        group_by_dd = html.Div(
            [create_group_by_dropdown(type_index, metric, segment, discovered_project)],
            className=COMPLEX_SEGMENT_FOOTER,
        )
        body_children.append(group_by_dd)

    card_body = dbc.CardBody(
        children=body_children,
        className=COMPLEX_SEGMENT_BODY + " fw-normal p-0",
    )
    return dbc.Card(
        id={"type": COMPLEX_SEGMENT, "index": type_index},
        children=[header, card_body],
        className=COMPLEX_SEGMENT + " p-0",
    )


def from_all_inputs(
    discovered_project: M.DiscoveredProject,
    complex_segment: Dict[str, Any],
) -> Optional[M.Segment]:
    res_segment: Optional[M.Segment] = None
    event_segments = complex_segment.get(CHILDREN, {})

    for _, event_segment in event_segments.items():
        event_segment = ES.from_all_inputs(discovered_project, event_segment)
        if event_segment is None:
            continue
        if res_segment is None:
            res_segment = event_segment
        else:
            res_segment = res_segment | event_segment

    return res_segment
