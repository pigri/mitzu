import logging
import os
from typing import Any, List, Optional, Union
from urllib.parse import ParseResult

import dash.development.base_component as bc
import mitzu.model as M
from dash import Dash, html

PROJECT_PATH_INDEX = 0

LOGGER = logging.getLogger()
LOGGER.setLevel(os.getenv("LOG_LEVEL", logging.INFO))


def value_to_label(value: str) -> str:
    return value.title().replace("_", " ")


def deserialize_component(val: Any) -> bc.Component:
    if type(val) == dict:

        namespace = val["namespace"]
        comp_type = val["type"]
        props = val["props"]
        children_dicts = props.get("children", [])

        if type(children_dicts) == list and len(children_dicts) > 0:
            props["children"] = [
                deserialize_component(child) for child in children_dicts
            ]
        elif type(children_dicts) == dict and len(children_dicts) > 0:
            props["children"] = [deserialize_component(children_dicts)]

        module = __import__(namespace)
        class_ = getattr(module, comp_type)
        res = class_(**props)

        return res
    else:
        return val


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


def find_first_component(
    id_or_type_id: str, among: Union[bc.Component, List[bc.Component]]
) -> bc.Component:
    if type(among) == list:
        for comp in among:
            res = find_first_component(id_or_type_id, comp)
            if res is not None:
                return res
    elif isinstance(among, bc.Component):
        id_type = type(getattr(among, "id", None))
        if (
            id_type == dict
            and getattr(among, "id", {}).get("type", None) == id_or_type_id
        ) or (id_type == str and getattr(among, "id", None) == id_or_type_id):
            return among

        return find_first_component(id_or_type_id, getattr(among, "children", []))
    return None


def find_components(
    type_id: str, among: Union[bc.Component, List[bc.Component]]
) -> List[bc.Component]:

    if type(among) == list:
        res = []
        for comp in among:
            res.extend(find_components(type_id, comp))
        return res
    elif isinstance(among, bc.Component):
        if getattr(among, "id", {}).get("type") == type_id:
            return [among]

        return find_components(type_id, getattr(among, "children", []))
    return []


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
