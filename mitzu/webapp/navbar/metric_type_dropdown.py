from dash import dcc, html

METRIC_TYPE_DROPDOWN = "metric-type-dropdown"
METRIC_TYPE_DROPDOWN_OPTION = "metric-type-dropdown-option"


TYPES = {
    "segmentation": "bi bi-graph-up",
    "conversion": "bi bi-filter-square",
    "retention": "bi bi-arrow-clockwise",
    "time_to_convert": "bi bi-clock-history",
    "journey": "bi bi-bezier2",
}

DEF_STYLE = {"font-size": 15, "padding-left": 10}


def value_to_label(value: str) -> str:
    return value.title().replace("_", " ")


def create_metric_type_dropdown():
    return dcc.Dropdown(
        options=[
            {
                "label": html.Div(
                    [
                        html.I(className=css_class),
                        html.Div(value_to_label(val), style=DEF_STYLE),
                    ],
                    className=METRIC_TYPE_DROPDOWN_OPTION,
                ),
                "value": val,
            }
            for val, css_class in TYPES.items()
        ],
        id=METRIC_TYPE_DROPDOWN,
        className=METRIC_TYPE_DROPDOWN,
        clearable=False,
        value="segmentation",
    )
