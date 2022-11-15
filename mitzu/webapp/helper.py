from __future__ import annotations

from typing import Any, Dict, List, Optional

import mitzu.model as M
from dash import html
from mitzu.helper import value_to_label


METRIC_SEGMENTS = "metric_segments"
CHILDREN = "children"


def get_enums(path: str, discovered_project: M.DiscoveredProject) -> List[Any]:
    event_field_def = find_event_field_def(path, discovered_project)
    if event_field_def is not None:
        res = event_field_def._enums
        return res if res is not None else []
    return []


def find_event_field_def(
    path: str, discovered_project: M.DiscoveredProject
) -> M.EventFieldDef:
    path_parts = path.split(".")
    event_name = path_parts[0]
    event_def = discovered_project.get_event_def(event_name)
    field_name = ".".join(path_parts[1:])

    for field, event_field_def in event_def._fields.items():
        if field._get_name() == field_name:
            return event_field_def
    raise Exception(f"Invalid property path: {path}")


def get_event_names(segment: Optional[M.Segment]) -> List[str]:
    if segment is None:
        return []
    if isinstance(segment, M.SimpleSegment):
        if segment._left is None:
            return []
        return [segment._left._event_name]
    elif isinstance(segment, M.ComplexSegment):
        return get_event_names(segment._left) + get_event_names(segment._right)
    else:
        raise Exception(f"Unsupported Segment Type: {type(segment)}")


def get_property_name_comp(field_name: str) -> html.Div:
    parts = field_name.split(".")
    if len(parts) == 1:
        return html.Div(value_to_label(field_name), className="property_name")
    return html.Div(
        [
            html.Div(
                value_to_label(".".join(parts[:-1])), className="property_name_prefix"
            ),
            html.Div(value_to_label(parts[-1]), className="property_name"),
        ],
    )


def get_final_all_inputs(
    all_inputs: Dict[str, Any], ctx_input_list: List[Dict]
) -> Dict[str, Any]:
    res: Dict[str, Any] = all_inputs
    res[METRIC_SEGMENTS] = {}
    for ipt in ctx_input_list:
        if type(ipt) == list:
            for sub_input in ipt:
                if type(sub_input) == dict:
                    sub_input_id = sub_input["id"]
                    index = sub_input_id["index"]
                    input_type = sub_input_id["type"]
                    curr = res[METRIC_SEGMENTS]
                    for sub_index in index.split("-"):
                        sub_index = int(sub_index)
                        if CHILDREN not in curr:
                            curr[CHILDREN] = {}
                        curr = curr[CHILDREN]
                        if sub_index not in curr:
                            curr[sub_index] = {}
                        curr = curr[sub_index]
                    curr[input_type] = sub_input["value"]
                else:
                    raise ValueError(f"Invalid sub-input type: {type(sub_input)}")

    return res
