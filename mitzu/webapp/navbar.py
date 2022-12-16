from __future__ import annotations

import dash.development.base_component as bc

import dash_bootstrap_components as dbc
from dash import html, callback
from dash.dependencies import State, Input, Output, ALL
from typing import List

NAVBAR_COLLAPSE = "navbar-collapse"
NAVBAR_TOGGLER = "navbar-toggler"
OFF_CANVAS_TOGGLER = "off-canvas-toggler"


def create_mitzu_navbar(
    id: str, children: List[bc.Component], off_canvas_toggler_visible: bool = True
) -> dbc.Navbar:
    navbar_children = [
        dbc.Col(
            dbc.Button(
                html.B(className="bi bi-list"),
                color="info",
                size="sm",
                className="me-1",
                id={"type": OFF_CANVAS_TOGGLER, "index": id},
                style={
                    "display": "inline-block" if off_canvas_toggler_visible else "none"
                },
            ),
        )
    ]
    navbar_children.extend([dbc.Col(comp) for comp in children])
    res = dbc.Navbar(
        dbc.Container(
            [
                dbc.Row(
                    children=navbar_children,
                    className="g-2",
                ),
            ],
            fluid=True,
        ),
        color="secondary",
    )

    @callback(
        Output(NAVBAR_COLLAPSE, "is_open"),
        [Input({"type": NAVBAR_TOGGLER, "index": ALL}, "n_clicks")],
        [State(NAVBAR_COLLAPSE, "is_open")],
    )
    def toggle_navbar_collapse(n, is_open):
        if n:
            return not is_open
        return is_open

    return res
