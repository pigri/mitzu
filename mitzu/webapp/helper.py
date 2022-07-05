from types import FunctionType
from typing import Any, Dict, List, Union

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


def get_enums(path: str, dataset_model: M.DatasetModel) -> Dict[str, Any]:
    curr = find_property_class(path=path, dataset_model=dataset_model)

    res = {}
    for k, v in curr.__class__.__dict__.items():
        if type(v) != M.SimpleSegment or k in ("is_null", "is_not_null"):
            continue
        res[str(v._right)] = v._right
    return res


def get_sub_items(name: str, value: Any) -> List[str]:
    if (
        name.startswith("_")
        or isinstance(value, M.SimpleSegment)
        or isinstance(value, FunctionType)
    ):
        return []

    sub_props = recursive_find_all_props(value)
    if len(sub_props) > 0:
        return [f"{name}.{sp}" for sp in sub_props]
    return [name]


def recursive_find_all_props(value: Any) -> List[str]:
    props = []
    for name, variable in value.__class__.__dict__.items():
        props.extend(get_sub_items(name, variable))

    for name, variable in value.__dict__.items():
        props.extend(get_sub_items(name, variable))

    return props


def find_property_class(path: str, dataset_model: M.DatasetModel) -> Any:
    curr = dataset_model
    steps = path.split(".")
    for step in steps:
        if step in curr.__class__.__dict__:
            curr = curr.__class__.__dict__[step]
        elif step in curr.__dict__:
            curr = curr.__dict__[step]
        else:
            raise ValueError(f"Incorrect Path {path}")
    return curr


def find_component(
    id: str, among: Union[bc.Component, List[bc.Component]]
) -> bc.Component:

    if type(among) == list:
        for comp in among:
            res = find_component(id, comp)
            if res is not None:
                return res
    elif isinstance(among, bc.Component):
        if getattr(among, "id", None) == id:
            return among

        return find_component(id, getattr(among, "children", []))

    return None
