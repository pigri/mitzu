from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

import mitzu.model as M
from dash import Dash, ctx, dcc, html
from dash.dependencies import MATCH, Input, Output, State
from dash.exceptions import PreventUpdate
from mitzu.webapp.helper import (
    deserialize_component,
    find_property_class,
    get_enums,
    recursive_find_all_props,
)

SIMPLE_SEGMENT = "simple_segment"
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
MULTI_OPTION_OPERATORS = ["is", "is not"]
CUSTOM_VAL_PREFIX = "$EQ$_"


def create_property_dropdown(
    event_name: str,
    index: str,
    dataset_model: M.DatasetModel,
    simple_segment_index: int,
) -> dcc.Dropdown:
    event = dataset_model.__class__.__dict__.get(event_name, [])
    placeholder = "Where ..." if simple_segment_index == 0 else "And ..."
    props = recursive_find_all_props(event)
    props.sort()
    return dcc.Dropdown(
        options=[{"label": k, "value": f"{event_name}.{k}"} for k in props],
        value=None,
        multi=False,
        placeholder=placeholder,
        className=PROPERTY_NAME_DROPDOWN,
        id={
            "type": PROPERTY_NAME_DROPDOWN,
            "index": index,
        },
    )


def create_value_input(
    index: str, path: str, dataset_model: M.DatasetModel, multi: bool = True
) -> dcc.Dropdown:
    enums = get_enums(path, dataset_model)
    options = [{"label": k, "value": v} for k, v in enums.items()]
    options.sort(key=lambda v: v["label"])
    options_str = (", ".join(enums.keys()))[0:20]
    if len(options_str) == 20:
        options_str = options_str + "..."
    return dcc.Dropdown(
        options=options,
        value=[] if multi else None,
        multi=multi,
        clearable=False,
        placeholder=options_str,
        className=PROPERTY_VALUE_INPUT,
        id={
            "type": PROPERTY_VALUE_INPUT,
            "index": index,
        },
    )


def create_property_operator_dropdown(index: str) -> dcc.Dropdown:
    return dcc.Dropdown(
        options=[k for k in OPERATOR_MAPPING.values()],
        value="is",
        multi=False,
        clearable=False,
        className=PROPERTY_OPERATOR_DROPDOWN,
        id={
            "type": PROPERTY_OPERATOR_DROPDOWN,
            "index": index,
        },
    )


def collect_values(values: List[str], options: List[Dict]) -> List[Any]:
    prefix_length = len(CUSTOM_VAL_PREFIX)
    return [
        val[prefix_length:] if val.startswith(CUSTOM_VAL_PREFIX) else val
        for val in values
    ]


class SimpleSegmentDiv(html.Div):
    def __init__(
        self, event_name: str, dataset_model: M.DatasetModel, simple_segment_index: int
    ):
        index = str(uuid4())
        prop_dd = create_property_dropdown(
            event_name, index, dataset_model, simple_segment_index
        )
        super().__init__(
            id={"type": SIMPLE_SEGMENT, "index": index},
            children=[prop_dd],
            className=SIMPLE_SEGMENT,
        )

    @classmethod
    def get_simple_segment(
        cls,
        simple_segment: html.Div,
        dataset_model: M.DatasetModel,
    ) -> Optional[M.Segment]:
        children = simple_segment.children
        if len(children) == 1:
            return None
        property_path: str = children[0].value
        property_operator: str = children[1].value
        property = find_property_class(dataset_model=dataset_model, path=property_path)
        if property_operator == OPERATOR_MAPPING[M.Operator.IS_NULL]:
            return property.is_null
        elif property_operator == OPERATOR_MAPPING[M.Operator.IS_NOT_NULL]:
            return property.is_not_null

        if children[2].value is None:
            return None

        if property_operator == OPERATOR_MAPPING[M.Operator.ANY_OF]:
            return property.any_of(
                *collect_values(children[2].value, children[2].options)
            )
        elif property_operator == OPERATOR_MAPPING[M.Operator.NONE_OF]:
            return property.not_any_of(
                *collect_values(children[2].value, children[2].options)
            )
        elif property_operator == OPERATOR_MAPPING[M.Operator.GT]:
            return property.gt(children[2].value)
        elif property_operator == OPERATOR_MAPPING[M.Operator.GT_EQ]:
            return property.gt_eq(children[2].value)
        elif property_operator == OPERATOR_MAPPING[M.Operator.LT]:
            return property.lt(children[2].value)
        elif property_operator == OPERATOR_MAPPING[M.Operator.LT_EQ]:
            return property.lt_eq(children[2].value)
        elif property_operator == OPERATOR_MAPPING[M.Operator.LIKE]:
            return property.like(children[2].value)
        elif property_operator == OPERATOR_MAPPING[M.Operator.NOT_LIKE]:
            return property.not_like(children[2].value)
        else:
            raise ValueError(f"Not supported Operator { property_operator }")

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
            dropdown = deserialize_component(children[2])
            options = dropdown.options
            values = dropdown.value
            options = [
                o
                for o in options
                if not o.get("value", "").startswith(CUSTOM_VAL_PREFIX)
                or (values is not None and o.get("value", "") in values)
            ]
            if search_value not in [o["label"] for o in options]:
                options.append(
                    {
                        "label": search_value,
                        "value": f"{CUSTOM_VAL_PREFIX}{search_value}",
                    }
                )
            return options

    @classmethod
    def fix(cls, simple_segment: html.Div, dataset_model: M.DatasetModel) -> html.Div:
        children = simple_segment.children
        prop_dd: dcc.Dropdown = children[0]
        index = prop_dd.id.get("index")

        trg_id = ctx.triggered_id
        if type(trg_id) == str:
            source_index = None
            source_type = trg_id
        else:
            source_type = trg_id.get("type")
            source_index = trg_id.get("index")

        if index == source_index and PROPERTY_NAME_DROPDOWN == source_type:
            children = [prop_dd]
        if index == source_index and PROPERTY_OPERATOR_DROPDOWN == source_type:
            children = [prop_dd, children[1]]

        if prop_dd.value is not None and len(children) == 1:
            # Add Operator Dropdown
            children.append(create_property_operator_dropdown(index))
        elif prop_dd.value is None and len(children) > 1:
            # Add Operator Dropdown Remove if no Event Name selected
            children = [prop_dd]

        if len(children) == 3 and children[1].value in NULL_OPERATORS:
            children = [prop_dd, children[1]]
        elif len(children) == 2 and children[1].value not in NULL_OPERATORS:
            children.append(
                create_value_input(
                    index=index,
                    path=prop_dd.value,
                    dataset_model=dataset_model,
                    multi=children[1].value in MULTI_OPTION_OPERATORS,
                )
            )

        simple_segment.children = children
        return simple_segment
