from __future__ import annotations

from typing import Any, Dict, List

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.dates_selector as DS
import mitzu.webapp.webapp as WA
from dash import dcc

METRICS_CONFIG = "metrics_config"

CONVERSION_WINDOW = "conversion_window"
CONVERSION_WINDOW_INTERVAL = "conversion_window_interval"
CONVERSION_WINDOW_INTERVAL_STEPS = "conversion_window_interval_steps"


def get_time_group_options(include_total: bool = True) -> List[Dict[str, int]]:
    res: List[Dict[str, Any]] = []
    for tg in M.TimeGroup:
        if not include_total and tg == M.TimeGroup.TOTAL:
            continue
        res.append({"label": tg.name.lower().title(), "value": tg.value})
    return res


def create_time_window_component() -> bc.Component:
    return dbc.InputGroup(
        id=CONVERSION_WINDOW,
        children=[
            dbc.InputGroupText("Conversion Window:"),
            dbc.Input(
                id=CONVERSION_WINDOW_INTERVAL,
                className=CONVERSION_WINDOW_INTERVAL,
                type="number",
                max=10000,
                min=1,
                value=1,
                size="sm",
                style={"max-width": "60px"},
            ),
            dcc.Dropdown(
                id=CONVERSION_WINDOW_INTERVAL_STEPS,
                className=CONVERSION_WINDOW_INTERVAL_STEPS,
                clearable=False,
                multi=False,
                value=M.TimeGroup.DAY.value,
                options=get_time_group_options(False),
                style={"width": "120px", "height": "38px"},
            ),
        ],
    )


class MetricsConfigCard(dbc.Card):
    def __init__(self):
        super().__init__(
            children=[
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                DS.create_date_selector(),
                                xs=12,
                                md=6,
                            ),
                            dbc.Col([create_time_window_component()], xs=12, md=6),
                        ]
                    )
                )
            ],
            id=METRICS_CONFIG,
            className=METRICS_CONFIG,
        )


def create_callbacks(webapp: WA.MitzuWebApp):
    DS.create_callbacks(webapp)
