from __future__ import annotations

import mitzu.webapp.webapp as WA
from dash import Input, Output, dcc, html
from mitzu.webapp.helper import value_to_label

METRIC_TYPE_DROPDOWN = "metric-type-dropdown"
METRIC_TYPE_DROPDOWN_OPTION = "metric-type-dropdown-option"

SEGMENTATION = "segmentation"
CONVERSION = "conversion"
RETENTION = "retention"
TIME_TO_CONVERT = "time_to_convert"
JOURNEY = "journey"

TYPES = {
    SEGMENTATION: "bi bi-graph-up",
    CONVERSION: "bi bi-filter-square",
    RETENTION: "bi bi-arrow-clockwise",
    TIME_TO_CONVERT: "bi bi-clock-history",
    JOURNEY: "bi bi-bezier2",
}

DEF_STYLE = {"font-size": 15, "padding-left": 10}


def create_metric_type_dropdown(webapp: WA.MitzuWebApp):
    res = dcc.Dropdown(
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
                "disabled": val not in [SEGMENTATION, CONVERSION],
            }
            for val, css_class in TYPES.items()
        ],
        id=METRIC_TYPE_DROPDOWN,
        className=METRIC_TYPE_DROPDOWN,
        clearable=False,
        value=SEGMENTATION,
        searchable=False,
    )

    @webapp.app.callback(
        Output(METRIC_TYPE_DROPDOWN, "value"),
        Input(WA.MITZU_LOCATION, "pathname"),
    )
    def update(curr_pathname: str):
        path_parts = curr_pathname.split("/")
        curr_metric_type = SEGMENTATION
        if len(path_parts) > WA.METRIC_TYPE_PATH_INDEX:
            curr_metric_type = path_parts[WA.METRIC_TYPE_PATH_INDEX]

        return curr_metric_type

    return res
