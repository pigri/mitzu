from __future__ import annotations

from typing import List, Optional

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
from dash import dcc, html
import dash_mantine_components as dmc

import mitzu.model as M
from mitzu.helper import value_to_label

CHART_TYPE_DD = "chart_type_button"
CHART_TYPE_CONTAINER = "chart_type_container"
GRAPH_CONTENT_TYPE = "graph_content_type"
GRAPH_REFRESH_BUTTON = "graph_refresh_button"
CANCEL_BUTTON = "cancel_button"

TOOLBAR_ROW = "toolbar_row"

VISIBLE = {"display": "inline-block", "margin-left": "8px"}
HIDDEN = {"display": "none"}

CHART_VAL = "Chart"
TABLE_VAL = "Table"
SQL_VAL = "SQL"


COPY_BUTTON = "copy_button"
CONTENT_CLIPBOARD = "content_clipboard"


def create_chart_type_dropdown_options(
    metric: Optional[M.Metric],
) -> List[M.SimpleChartType]:
    if metric is None:
        return [M.SimpleChartType.LINE]
    if metric._time_group == M.TimeGroup.TOTAL:
        options = [
            M.SimpleChartType.BAR,
        ]
        if isinstance(metric, M.SegmentationMetric):
            options.extend([M.SimpleChartType.STACKED_BAR])
        if isinstance(metric, M.RetentionMetric):
            options.insert(0, M.SimpleChartType.LINE)
    else:
        options = [
            M.SimpleChartType.LINE,
            M.SimpleChartType.BAR,
            M.SimpleChartType.HEATMAP,
        ]
        if isinstance(metric, M.SegmentationMetric):
            options.extend(
                [
                    M.SimpleChartType.STACKED_BAR,
                    M.SimpleChartType.STACKED_AREA,
                ]
            )
    return options


def create_chart_type_dropdown(metric: Optional[M.Metric]) -> dcc.Dropdown:
    options = create_chart_type_dropdown_options(metric)
    if metric is not None and metric._chart_type in options:
        ct = metric._chart_type
    else:
        ct = options[0]

    return dmc.Select(
        data=[{"label": value_to_label(o.name), "value": o.name} for o in options],
        value=ct.name,
        id=CHART_TYPE_DD,
        size="xs",
        className="me-1 btn-secondary",
        style={
            "max-width": "120px",
            "display": "inline-block",
        },
    )


def from_metric(metric: Optional[M.Metric]) -> bc.Component:
    comp = dbc.Row(
        id=TOOLBAR_ROW,
        children=[
            html.Hr(),
            dbc.Col(
                children=[
                    dbc.Button(
                        children=[html.B(className="bi bi-arrow-clockwise")],
                        size="sm",
                        color="primary",
                        id=GRAPH_REFRESH_BUTTON,
                        disabled=False,
                        class_name="me-1",
                    ),
                    dbc.Button(
                        children=[
                            dbc.Spinner(size="sm", color="dark", type="border"),
                            " Cancel",
                        ],
                        size="sm",
                        color="secondary",
                        id=CANCEL_BUTTON,
                        style=HIDDEN,
                    ),
                ],
            ),
            dbc.Col(
                children=[
                    html.Div(
                        children=[create_chart_type_dropdown(metric)],
                        id=CHART_TYPE_CONTAINER,
                        className="d-inline-block",
                    ),
                    dmc.SegmentedControl(
                        data=[CHART_VAL, TABLE_VAL, SQL_VAL],
                        size="xs",
                        id=GRAPH_CONTENT_TYPE,
                        className="me-1 bg-transparent",
                        value=CHART_VAL,
                    ),
                    dbc.Button(
                        [
                            html.B(className="bi bi-link-45deg me-1"),
                            "Copy",
                        ],
                        id=COPY_BUTTON,
                        color="light",
                        size="sm",
                    ),
                ],
            ),
        ],
        class_name="mt-3",
    )

    return comp
