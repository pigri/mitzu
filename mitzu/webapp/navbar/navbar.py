from __future__ import annotations

import dash_bootstrap_components as dbc
import mitzu.webapp.webapp as WA
from dash import Dash, Input, Output, State, html
from mitzu.webapp.navbar.metric_type_dropdown import create_metric_type_dropdown
from mitzu.webapp.navbar.project_dropdown import create_project_dropdown

LOGO = "/assets/favicon_io/round-android-chrome-192x192.png"

METRIC_TYPE_DROPDOWN = "metric_type_dropdown"
NAVBAR_COLLAPSE = "navbar-collapse"


def create_mitzu_navbar(webapp: WA.MitzuWebApp) -> dbc.Navbar:
    res = dbc.Navbar(
        children=dbc.Container(
            children=[
                dbc.Row(
                    children=[
                        dbc.Col(
                            html.A(
                                # Use row and col to control vertical alignment of logo / brand
                                children=[html.Img(src=LOGO, height="32px")],
                                href="/",
                                style={"textDecoration": "none"},
                            ),
                        ),
                    ]
                ),
                dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                dbc.Collapse(
                    children=[
                        dbc.Row(
                            [
                                dbc.Col(create_metric_type_dropdown()),
                                dbc.Col(create_project_dropdown(webapp)),
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
                                        align_end=True,
                                        in_navbar=True,
                                    ),
                                ),
                            ],
                            className="g-0 ms-auto flex-nowrap mt-1 mt-md-0",
                            align="center",
                        ),
                    ],
                    id=NAVBAR_COLLAPSE,
                    is_open=False,
                    navbar=True,
                ),
            ],
            fluid=True,
        ),
        sticky="top",
    )

    # add callback for toggling the collapse on small screens
    @webapp.app.callback(
        Output("navbar-collapse", "is_open"),
        [Input("navbar-toggler", "n_clicks")],
        [State("navbar-collapse", "is_open")],
    )
    def toggle_navbar_collapse(n, is_open):
        if n:
            return not is_open
        return is_open

    return res
