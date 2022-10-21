import logging
import os
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import ParseResult

import mitzu.model as M
from dash import Dash, html

PROJECT_PATH_INDEX = 0
METRIC_SEGMENTS = "metric_segments"
CHILDREN = "children"

LOGGER = logging.getLogger()
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOGGER.setLevel(os.getenv("LOG_LEVEL", logging.INFO))


def value_to_label(value: str) -> str:
    return value.title().replace("_", " ")


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


def get_path_project_name(url_parse_result: ParseResult, dash: Dash) -> Optional[str]:
    fixed_path = url_parse_result.path
    if not fixed_path.startswith("/"):
        fixed_path = f"/{fixed_path}"
    fixed_path = dash.strip_relative_path(fixed_path)
    path_parts = fixed_path.split("/")
    return path_parts[PROJECT_PATH_INDEX]


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


def transform_all_inputs(all_inputs: List[Any]) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    res[METRIC_SEGMENTS] = {}
    for ipt in all_inputs:
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
        else:
            res[ipt["id"]] = ipt.get("value")
    return res
