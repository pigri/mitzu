from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.webapp.webapp as WA
from dash import Input, Output, ctx, html

CHART_BUTTON = "chart_button"
TABLE_BUTTON = "table_button"
SQL_BUTTON = "sql_button"
GRAPH_REFRESH_BUTTON = "graph_refresh_button"
CANCEL_BUTTON = "cancel_button"

TOOLBAR_ROW = "toolbar_row"
TOOLBAR_LEFT = "toolbar_left"
TOOLBAR_CENTER = "toolbar_center"
TOOLBAR_RIGHT = "toolbar_right"

VISIBLE = {"display": "inline-block", "margin-left": "8px"}
HIDDEN = {"display": "none"}


@dataclass
class ToolbarHandler:

    component: bc.Component
    webapp: WA.MitzuWebApp

    @classmethod
    def from_webapp(cls, webapp: WA.MitzuWebApp) -> ToolbarHandler:
        comp = dbc.Row(
            id=TOOLBAR_ROW,
            class_name=TOOLBAR_ROW,
            children=[
                dbc.Col(
                    id=TOOLBAR_LEFT,
                    class_name=TOOLBAR_LEFT,
                    children=[
                        dbc.Button(
                            children=[html.B(className="bi bi-arrow-clockwise")],
                            size="sm",
                            color="info",
                            id=GRAPH_REFRESH_BUTTON,
                            disabled=False,
                        ),
                        dbc.Button(
                            children=[
                                dbc.Spinner(size="sm", color="dark", type="border"),
                                " Cancel",
                            ],
                            size="sm",
                            color="light",
                            id=CANCEL_BUTTON,
                            style=HIDDEN,
                            outline=True,
                        ),
                    ],
                ),
                dbc.Col(
                    id=TOOLBAR_RIGHT,
                    class_name=TOOLBAR_RIGHT,
                    children=dbc.InputGroup(
                        children=[
                            dbc.Button(
                                children=[
                                    html.B(
                                        className="bi bi-graph-up",
                                    ),
                                ],
                                size="sm",
                                outline=False,
                                id=CHART_BUTTON,
                                color="info",
                            ),
                            dbc.Button(
                                children=[
                                    html.B(
                                        className="bi bi-grid-3x3",
                                    ),
                                ],
                                size="sm",
                                outline=False,
                                id=TABLE_BUTTON,
                                color="light",
                            ),
                            dbc.Button(
                                children=[
                                    html.B(
                                        className="bi bi-braces-asterisk",
                                    ),
                                    " SQL",
                                ],
                                size="sm",
                                outline=False,
                                id=SQL_BUTTON,
                                color="light",
                            ),
                        ],
                    ),
                ),
            ],
        )

        return ToolbarHandler(component=comp, webapp=webapp)

    def create_callbacks(self):
        @self.webapp.app.callback(
            output=[
                Output(CHART_BUTTON, "color"),
                Output(TABLE_BUTTON, "color"),
                Output(SQL_BUTTON, "color"),
            ],
            inputs=[
                Input(CHART_BUTTON, "n_clicks"),
                Input(TABLE_BUTTON, "n_clicks"),
                Input(SQL_BUTTON, "n_clicks"),
            ],
            prevent_initial_call=True,
        )
        def toggle_viz(
            chart_n_clicks: int, table_n_clicks: int, sql_n_clicks: int
        ) -> Tuple[str, str, str]:

            return (
                "info" if ctx.triggered_id == CHART_BUTTON else "light",
                "info" if ctx.triggered_id == TABLE_BUTTON else "light",
                "info" if ctx.triggered_id == SQL_BUTTON else "light",
            )
