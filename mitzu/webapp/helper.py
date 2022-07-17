from typing import Any, List, Optional, Union

import dash.development.base_component as bc
import mitzu.model as M


def value_to_label(value: str) -> str:
    return value.title().replace("_", " ")


def deserialize_component(val: Any) -> bc.Component:
    if type(val) == dict:

        namespace = val["namespace"]
        comp_type = val["type"]
        props = val["props"]
        children_dicts = props.get("children", [])

        if type(children_dicts) == list:
            props["children"] = [
                deserialize_component(child) for child in children_dicts
            ]
        elif type(children_dicts) == dict:
            props["children"] = [deserialize_component(children_dicts)]

        module = __import__(namespace)
        class_ = getattr(module, comp_type)
        res = class_(**props)

        return res
    else:
        return val


def get_enums(
    path: str, discovered_datasource: M.DiscoveredEventDataSource
) -> List[Any]:
    event_field_def = find_event_field_def(path, discovered_datasource)
    if event_field_def is not None:
        res = event_field_def._enums
        return res if res is not None else []
    return []


def find_event_field_def(
    path: str, discovered_datasource: M.DiscoveredEventDataSource
) -> Optional[M.EventFieldDef]:
    path_parts = path.split(".")
    event_name = path_parts[0]
    event_def = discovered_datasource.get_all_events()[event_name]
    field_name = ".".join(path_parts[1:])

    for field, event_field_def in event_def._fields.items():
        if field._get_name() == field_name:
            return event_field_def
    return None


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
