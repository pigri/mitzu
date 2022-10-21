from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import dash.development.base_component as bc
import mitzu.model as M
import mitzu.webapp.webapp as WA
from dash import dcc, html
from dash.dependencies import MATCH, Input, Output, State
from dash.exceptions import PreventUpdate
from mitzu.webapp.helper import find_event_field_def, get_enums, get_property_name_comp

SIMPLE_SEGMENT = "simple_segment"
SIMPLE_SEGMENT_WITH_VALUE = "simple_segment_with_value"
PROPERTY_NAME_DROPDOWN = "property_name_dropdown"
PROPERTY_OPERATOR_DROPDOWN = "property_operator_dropdown"
PROPERTY_VALUE_INPUT = "property_value_input"


OPERATOR_MAPPING = {
    M.Operator.ANY_OF: "is",
    M.Operator.NONE_OF: "is not",
    M.Operator.GT: ">",
    M.Operator.GT_EQ: ">=",
    M.Operator.LT: "<",
    M.Operator.LT_EQ: "<=",
    M.Operator.IS_NOT_NULL: "present",
    M.Operator.IS_NULL: "missing",
    M.Operator.LIKE: "like",
    M.Operator.NOT_LIKE: "not like",
}

NULL_OPERATORS = ["present", "missing"]
MULTI_OPTION_OPERATORS = [M.Operator.ANY_OF, M.Operator.NONE_OF]
BOOL_OPERATORS = [M.Operator.IS_NOT_NULL, M.Operator.IS_NULL]
CUSTOM_VAL_PREFIX = "$EQ$_"


def create_property_dropdown(
    simple_segment: M.SimpleSegment,
    discovered_project: M.DiscoveredProject,
    simple_segment_index: int,
    type_index: str,
) -> dcc.Dropdown:
    event_name = simple_segment._left._event_name
    field_name: Optional[str] = None
    if type(simple_segment._left) == M.EventFieldDef:
        field_name = simple_segment._left._field._get_name()

    event = discovered_project.get_event_def(event_name)
    placeholder = "+ Where" if simple_segment_index == 0 else "+ And"
    fields_names = [f._get_name() for f in event._fields.keys()]
    fields_names.sort()
    options = [
        {"label": get_property_name_comp(f), "value": f"{event_name}.{f}"}
        for f in fields_names
    ]

    return dcc.Dropdown(
        options=options,
        value=None if field_name is None else f"{event_name}.{field_name}",
        multi=False,
        placeholder=placeholder,
        searchable=True,
        className=PROPERTY_NAME_DROPDOWN,
        id={
            "type": PROPERTY_NAME_DROPDOWN,
            "index": type_index,
        },
    )


def create_value_input(
    simple_segment: M.SimpleSegment,
    discovered_project: M.DiscoveredProject,
    type_index: str,
) -> dcc.Dropdown:
    multi = simple_segment._operator in MULTI_OPTION_OPERATORS
    value = simple_segment._right
    left = simple_segment._left

    if type(left) == M.EventFieldDef:
        path = f"{left._event_name}.{left._field._get_name()}"
        enums = get_enums(path, discovered_project)
        if value is not None:
            if type(value) in (list, tuple):
                enums = list(set([*list(value), *enums]))
            else:
                enums = [value, *enums]
    else:
        enums = []

    options = [{"label": str(e), "value": e} for e in enums]
    options.sort(key=lambda v: v["label"])

    placeholder = (", ".join([str(e) for e in enums]))[0:20] + "..."

    comp_value: Any = value
    if multi:
        if value is not None and type(value) in (list, tuple):
            comp_value = list(value)
        if value is None:
            comp_value = []

    return dcc.Dropdown(
        options=options,
        value=comp_value,
        multi=multi,
        clearable=False,
        searchable=True,
        placeholder=placeholder,
        className=PROPERTY_VALUE_INPUT,
        id={
            "type": PROPERTY_VALUE_INPUT,
            "index": type_index,
        },
        style={"width": "100%"},
    )


def create_property_operator_dropdown(
    simple_segment: M.SimpleSegment, type_index: str
) -> dcc.Dropdown:
    options: List[str] = []
    if type(simple_segment._left) == M.EventFieldDef:
        data_type = simple_segment._left._field._type
        if data_type == M.DataType.BOOL:
            options = [
                OPERATOR_MAPPING[k]
                for k in [
                    M.Operator.ANY_OF,
                    M.Operator.NONE_OF,
                    M.Operator.IS_NOT_NULL,
                    M.Operator.IS_NULL,
                ]
            ]
        elif data_type == M.DataType.NUMBER:
            options = [
                OPERATOR_MAPPING[k]
                for k in [
                    M.Operator.ANY_OF,
                    M.Operator.NONE_OF,
                    M.Operator.GT,
                    M.Operator.GT_EQ,
                    M.Operator.LT,
                    M.Operator.LT_EQ,
                    M.Operator.IS_NOT_NULL,
                    M.Operator.IS_NULL,
                ]
            ]
        else:
            options = [k for k in OPERATOR_MAPPING.values()]

    return dcc.Dropdown(
        options=options,
        value=(
            OPERATOR_MAPPING[M.Operator.ANY_OF]
            if simple_segment._operator is None
            else OPERATOR_MAPPING[simple_segment._operator]
        ),
        multi=False,
        searchable=False,
        clearable=False,
        className=PROPERTY_OPERATOR_DROPDOWN,
        id={
            "type": PROPERTY_OPERATOR_DROPDOWN,
            "index": type_index,
        },
    )


def fix_custom_value(val: Any, data_type: M.DataType):
    if type(val) == str:
        if val.startswith(CUSTOM_VAL_PREFIX):
            prefix_length = len(CUSTOM_VAL_PREFIX)
            val = val[prefix_length:]
        val = data_type.from_string(val)
    return val


def collect_values(value: Any, data_type: M.DataType) -> Optional[Tuple[Any, ...]]:
    if value is None:
        return None
    if type(value) in (list, tuple):
        return tuple([fix_custom_value(v, data_type) for v in value])
    else:
        return tuple([fix_custom_value(value, data_type)])


def from_all_inputs(
    discovered_project: Optional[M.DiscoveredProject],
    simple_segment: Dict[str, Any],
) -> Optional[M.SimpleSegment]:

    if discovered_project is None:
        return None

    property_path = simple_segment.get(PROPERTY_NAME_DROPDOWN)
    if property_path is None:
        return None

    event_field_def = find_event_field_def(property_path, discovered_project)
    property_operator = simple_segment.get(PROPERTY_OPERATOR_DROPDOWN)
    if property_operator is None:
        return M.SimpleSegment(event_field_def, M.Operator.ANY_OF, None)

    if property_operator == OPERATOR_MAPPING[M.Operator.IS_NULL]:
        return M.SimpleSegment(event_field_def, M.Operator.IS_NULL, None)
    elif property_operator == OPERATOR_MAPPING[M.Operator.IS_NOT_NULL]:
        return M.SimpleSegment(event_field_def, M.Operator.IS_NOT_NULL, None)

    property_value = simple_segment.get(PROPERTY_VALUE_INPUT)
    data_type = event_field_def._field._type

    if property_operator == OPERATOR_MAPPING[M.Operator.ANY_OF]:
        return M.SimpleSegment(
            event_field_def,
            M.Operator.ANY_OF,
            collect_values(property_value, data_type),
        )
    elif property_operator == OPERATOR_MAPPING[M.Operator.NONE_OF]:
        return M.SimpleSegment(
            event_field_def,
            M.Operator.NONE_OF,
            collect_values(property_value, data_type),
        )
    else:
        if type(property_value) == list:
            property_value = None
        for op, op_str in OPERATOR_MAPPING.items():
            if op_str == property_operator:
                fixed_val = fix_custom_value(property_value, data_type)
                if fixed_val == []:
                    fixed_val = None
                return M.SimpleSegment(event_field_def, op, fixed_val)

        raise ValueError(f"Not supported Operator { property_operator }")


def from_simple_segment(
    simple_segment: M.SimpleSegment,
    discovered_project: M.DiscoveredProject,
    parent_type_index: str,
    simple_segment_index: int,
) -> bc.Component:
    type_index = f"{parent_type_index}-{simple_segment_index}"
    prop_dd = create_property_dropdown(
        simple_segment, discovered_project, simple_segment_index, type_index
    )
    children = [prop_dd]
    if simple_segment._operator is not None:
        operator_dd = create_property_operator_dropdown(simple_segment, type_index)
        children.append(operator_dd)
        if simple_segment._operator not in BOOL_OPERATORS:
            value_input = create_value_input(
                simple_segment, discovered_project, type_index
            )
            children.append(value_input)

    component = html.Div(
        id={"type": SIMPLE_SEGMENT, "index": type_index},
        children=children,
        className=(
            SIMPLE_SEGMENT
            if simple_segment._left is None
            or isinstance(simple_segment._left, M.EventDef)
            else SIMPLE_SEGMENT_WITH_VALUE
        ),
    )

    return component


def create_callbacks(webapp: WA.MitzuWebApp):
    @webapp.app.callback(
        Output({"type": PROPERTY_VALUE_INPUT, "index": MATCH}, "options"),
        Input({"type": PROPERTY_VALUE_INPUT, "index": MATCH}, "search_value"),
        State({"type": PROPERTY_VALUE_INPUT, "index": MATCH}, "options"),
        State({"type": PROPERTY_VALUE_INPUT, "index": MATCH}, "value"),
        prevent_initial_call=True,
    )
    def update_options(search_value: str, options: List, values: List) -> List[str]:
        if search_value is None or search_value == "":
            raise PreventUpdate

        options = [
            o
            for o in options
            if type(o.get("value", "")) != str
            or not o.get("value", "").startswith(CUSTOM_VAL_PREFIX)
            or ((type(values) in (list, tuple) and o.get("value", "") in values))
        ]

        if search_value not in [o["label"] for o in options]:
            options.insert(
                0,
                {
                    "label": search_value,
                    "value": f"{CUSTOM_VAL_PREFIX}{search_value}",
                },
            )
        return options
