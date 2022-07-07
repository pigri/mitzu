from __future__ import annotations

import dash_bootstrap_components as dbc
import mitzu.webapp.navbar.metric_type_dropdown as MTD
import mitzu.webapp.navbar.project_dropdown as PD
import mitzu.webapp.webapp as WA
from dash import Input, Output, State, html

LOGO = "/assets/logo.png"


NAVBAR_COLLAPSE = "navbar-collapse"
NAVBAR_TOGGLER = "navbar-toggler"


def create_mitzu_navbar(webapp: WA.MitzuWebApp) -> dbc.Navbar:
    res = dbc.Navbar(
        children=dbc.Container(
            children=[
                dbc.NavbarBrand(
                    dbc.Row(
                        children=[
                            dbc.Col(
                                html.A(
                                    # Use row and col to control vertical alignment of logo / brand
                                    children=[html.Img(src=LOGO, height="32px")],
                                    href="/",
                                    style={"textDecoration": "none"},
                                )
                            ),
                            dbc.Col(PD.create_project_dropdown(webapp)),
                            dbc.Col(MTD.create_metric_type_dropdown(webapp)),
                        ]
                    ),
                ),
                dbc.Row(
                    children=[
                        dbc.Col(
                            dbc.DropdownMenu(
                                children=[
                                    dbc.DropdownMenuItem("Link"),
                                    dbc.DropdownMenuItem("CSV"),
                                    dbc.DropdownMenuItem("SQL Query"),
                                    dbc.DropdownMenuItem("PNG"),
                                ],
                                label="Share",
                                size="sm",
                                color="primary",
                                in_navbar=True,
                                align_end=True,
                            ),
                        ),
                        dbc.Col(
                            dbc.Button(
                                children=html.I(className="bi bi-gear"),
                                size="sm",
                                color="dark",
                            )
                        ),
                    ],
                    align="center",
                    justify="end",
                ),
            ],
            fluid=True,
        ),
        sticky="top",
    )

    # add callback for toggling the collapse on small screens
    @webapp.app.callback(
        Output(NAVBAR_COLLAPSE, "is_open"),
        [Input(NAVBAR_TOGGLER, "n_clicks")],
        [State(NAVBAR_COLLAPSE, "is_open")],
    )
    def toggle_navbar_collapse(n, is_open):
        if n:
            return not is_open
        return is_open

    return res
