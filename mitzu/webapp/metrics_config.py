from __future__ import annotations

from typing import Any, Dict, List

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
from dash import dcc, html

METRICS_CONFIG = "metrics_config"
TIME_GROUP_DROWDOWN = "timegroup_dropdown"
TIME_WINDOW = "time_window"
TIME_WINDOW_INTERVAL = "time_window_interval"
TIME_WINDOW_INTERVAL_STEPS = "time_window_interval_steps"
DATE_RANGE_INPUT = "date_range_input"


def get_time_group_options(include_total: bool = True) -> List[Dict[str, int]]:
    res: List[Dict[str, Any]] = []
    for tg in M.TimeGroup:
        if not include_total and tg == M.TimeGroup.TOTAL:
            continue
        res.append({"label": tg.name.lower().title(), "value": tg.value})
    return res


def create_time_group_dropdown() -> dcc.Dropdown:
    return html.Div(
        children=[
            "Time group:",
            dcc.Dropdown(
                id=TIME_GROUP_DROWDOWN,
                options=get_time_group_options(),
                value=M.TimeGroup.DAY.value,
                clearable=False,
                className=TIME_GROUP_DROWDOWN,
                multi=False,
            ),
        ]
    )


def create_time_window_component() -> bc.Component:
    return html.Div(
        children=[
            "Conversion window:",
            html.Div(
                id=TIME_WINDOW,
                className=TIME_WINDOW,
                children=[
                    dbc.Input(
                        id=TIME_WINDOW_INTERVAL,
                        className=TIME_WINDOW_INTERVAL,
                        type="number",
                        max=10000,
                        min=1,
                        value=1,
                        size="sm",
                    ),
                    dcc.Dropdown(
                        id=TIME_WINDOW_INTERVAL_STEPS,
                        className=TIME_WINDOW_INTERVAL_STEPS,
                        clearable=False,
                        multi=False,
                        value=M.TimeGroup.DAY.value,
                        options=get_time_group_options(False),
                    ),
                ],
            ),
        ]
    )


def create_start_dt_input():
    return html.Div(
        children=[
            "Custom date range",
            html.Div(
                children=[
                    dcc.DatePickerRange(
                        clearable=True,
                        display_format="YYYY-MM-DD",
                        id=DATE_RANGE_INPUT,
                        className=DATE_RANGE_INPUT,
                        start_date=None,
                        end_date=None,
                        number_of_months_shown=2,
                    )
                ]
            ),
        ]
    )


class MetricsConfigCard(dbc.Card):
    def __init__(self):
        super().__init__(
            children=[
                dbc.CardBody(
                    children=dbc.Row(
                        children=[
                            dbc.Col(create_start_dt_input()),
                            dbc.Col(create_time_group_dropdown()),
                            dbc.Col(create_time_window_component()),
                        ]
                    )
                )
            ],
            id=METRICS_CONFIG,
            className=METRICS_CONFIG,
        )
