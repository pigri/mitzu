from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Tuple

import mitzu.model as M
from dash import Dash, dcc, html
from dash.dependencies import MATCH, Input, Output, State
from dash.exceptions import PreventUpdate
from mitzu.webapp.helper import (
    deserialize_component,
    find_event_field_def,
    get_enums,
    value_to_label,
)

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
    discovered_datasource: M.DiscoveredEventDataSource,
    simple_segment_index: int,
    type_index: str,
) -> dcc.Dropdown:
    event_name = simple_segment._left._event_name
    field_name: Optional[str] = None
    if type(simple_segment._left) == M.EventFieldDef:
        field_name = simple_segment._left._field._get_name()

    event = discovered_datasource.get_event_def(event_name)
    placeholder = "+ Where" if simple_segment_index == 0 else "+ And"
    fields_names = [f._get_name() for f in event._fields.keys()]
    fields_names.sort()
    options = [
        {"label": value_to_label(f).split(".")[-1], "value": f"{event_name}.{f}"}
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
    discovered_datasource: M.DiscoveredEventDataSource,
    type_index: str,
) -> dcc.Dropdown:
    multi = simple_segment._operator in MULTI_OPTION_OPERATORS
    if type(simple_segment._left) == M.EventFieldDef:
        path = f"{simple_segment._left._event_name}.{simple_segment._left._field._get_name()}"
        enums = get_enums(path, discovered_datasource)
    else:
        enums = []

    options = [{"label": str(enum), "value": enum} for enum in enums]
    options.sort(key=lambda v: v["label"])

    if simple_segment._right is not None and simple_segment._right not in enums:
        options.insert(
            0, {"label": str(simple_segment._right), "value": simple_segment._right}
        )

    options_str = (", ".join([str(e) for e in enums]))[0:20] + "..."

    value = simple_segment._right
    if value is not None and type(value) == Tuple:
        value = list(value)
    if multi and value is None:
        value = []

    return dcc.Dropdown(
        options=options,
        value=value,
        multi=multi,
        clearable=False,
        searchable=True,
        placeholder=options_str,
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
    return dcc.Dropdown(
        options=[k for k in OPERATOR_MAPPING.values()],
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


def fix_custom_value(val: Any):
    if type(val) == str and val.startswith(CUSTOM_VAL_PREFIX):
        prefix_length = len(CUSTOM_VAL_PREFIX)
        return val[prefix_length:]
    else:
        return val


def collect_values(values: Any) -> List[Any]:
    if isinstance(values, Iterable):
        return [fix_custom_value(val) for val in values]
    else:
        return []


@dataclass
class SimpleSegmentHandler:

    discovered_datasource: M.DiscoveredEventDataSource
    component: html.Div

    def to_simple_segment(self) -> Optional[M.SimpleSegment]:
        children = self.component.children
        property_path: str = children[0].value
        if property_path is None:
            return None
        event_field_def = find_event_field_def(
            property_path, self.discovered_datasource
        )
        if len(children) == 1:
            return M.SimpleSegment(event_field_def, M.Operator.ANY_OF, None)

        property_operator: str = children[1].value
        if property_operator == OPERATOR_MAPPING[M.Operator.IS_NULL]:
            return M.SimpleSegment(event_field_def, M.Operator.IS_NULL, None)
        elif property_operator == OPERATOR_MAPPING[M.Operator.IS_NOT_NULL]:
            return M.SimpleSegment(event_field_def, M.Operator.IS_NOT_NULL, None)

        value = children[2].value if len(children) == 3 else None

        if property_operator == OPERATOR_MAPPING[M.Operator.ANY_OF]:
            return M.SimpleSegment(
                event_field_def,
                M.Operator.ANY_OF,
                tuple(collect_values(value)),
            )
        elif property_operator == OPERATOR_MAPPING[M.Operator.NONE_OF]:
            return M.SimpleSegment(
                event_field_def,
                M.Operator.NONE_OF,
                tuple(collect_values(value)),
            )
        else:
            for op, op_str in OPERATOR_MAPPING.items():
                if op_str == property_operator:
                    fixed_val = fix_custom_value(value)
                    print(fixed_val)
                    return M.SimpleSegment(event_field_def, op, fixed_val)

            raise ValueError(f"Not supported Operator { property_operator }")

    @classmethod
    def from_component(
        cls, component: html.Div, discovered_datasource: M.DiscoveredEventDataSource
    ) -> SimpleSegmentHandler:
        return SimpleSegmentHandler(discovered_datasource, component)

    @classmethod
    def from_simple_segment(
        cls,
        simple_segment: M.SimpleSegment,
        discovered_datasource: M.DiscoveredEventDataSource,
        parent_type_index: str,
        simple_segment_index: int,
    ) -> SimpleSegmentHandler:
        type_index = f"{parent_type_index}-{simple_segment_index}"
        prop_dd = create_property_dropdown(
            simple_segment, discovered_datasource, simple_segment_index, type_index
        )
        children = [prop_dd]
        if simple_segment._operator is not None:
            operator_dd = create_property_operator_dropdown(simple_segment, type_index)
            children.append(operator_dd)
            if simple_segment._operator not in BOOL_OPERATORS:
                value_input = create_value_input(
                    simple_segment, discovered_datasource, type_index
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

        return SimpleSegmentHandler(discovered_datasource, component)

    @classmethod
    def create_callbacks(cls, app: Dash):
        @app.callback(
            Output({"type": PROPERTY_VALUE_INPUT, "index": MATCH}, "options"),
            Input({"type": PROPERTY_VALUE_INPUT, "index": MATCH}, "search_value"),
            State({"type": SIMPLE_SEGMENT, "index": MATCH}, "children"),
            prevent_initial_call=True,
        )
        def update_options(search_value, children) -> List[str]:
            if search_value is None or search_value == "" or len(children) != 3:
                raise PreventUpdate
            value_dropdown = deserialize_component(children[2])
            options = value_dropdown.options
            values = value_dropdown.value
            options = [
                o
                for o in options
                if not o.get("value", "").startswith(CUSTOM_VAL_PREFIX)
                or (values is not None and o.get("value", "") in values)
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
