from __future__ import annotations

import os

import dash_bootstrap_components as dbc
import mitzu.webapp.navbar.metric_type_handler as MNB
import mitzu.webapp.navbar.project_dropdown as PD
import mitzu.webapp.webapp as WA
from dash import html, dcc
from dash.dependencies import State, Input, Output

SHARE_BUTTON = "share_button"
CLIPBOARD = "share_clipboard"
MANAGE_PROJECTS_LINK = os.getenv("MANAGE_PROJECTS_LINK")
SIGN_OUT_URL = os.getenv("SIGN_OUT_URL")
DASH_LOGO_PATH = os.getenv("DASH_LOGO_PATH", "assets/mitzu-logo-light.svg")
LOGO = "navbar_logo"
MORE_DD = "navbar_more_dropdown"


def create_mitzu_navbar(webapp: WA.MitzuWebApp) -> dbc.Navbar:

    res = dbc.Navbar(
        dbc.Container(
            [
                html.A(
                    children=dbc.Row(
                        dbc.Col(
                            html.Img(
                                src=DASH_LOGO_PATH, height="32px", className="logo"
                            )
                        )
                    ),
                    id=LOGO,
                    href="/",
                    style={"textDecoration": "none"},
                ),
                dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                dbc.Collapse(
                    dbc.Row(
                        [
                            dbc.Col(PD.create_project_dropdown(webapp)),
                            dbc.Col(MNB.from_metric_type(MNB.MetricType.SEGMENTATION)),
                            dbc.Col(
                                dbc.Button(
                                    [
                                        html.B(className="bi bi-link-45deg"),
                                        " Share",
                                        dcc.Clipboard(
                                            id=CLIPBOARD,
                                            content="",
                                            className="position-absolute start-0 top-0 w-100 h-100 opacity-0",
                                        ),
                                    ],
                                    id=SHARE_BUTTON,
                                    className="position-relative top-0 start-0 text-nowrap",
                                    color="success",
                                    size="sm",
                                )
                            ),
                            dbc.Col(
                                dbc.DropdownMenu(
                                    [
                                        dbc.DropdownMenuItem("Projects", header=True),
                                        dbc.DropdownMenuItem(
                                            "Manage projects",
                                            external_link=True,
                                            href=MANAGE_PROJECTS_LINK,
                                            target="_blank",
                                            disabled=(MANAGE_PROJECTS_LINK is None),
                                        ),
                                        dbc.DropdownMenuItem(divider=True),
                                        dbc.DropdownMenuItem(
                                            "Sign out",
                                            disabled=(SIGN_OUT_URL is None),
                                            href=SIGN_OUT_URL,
                                        ),
                                    ],
                                    id=MORE_DD,
                                    label="More",
                                    size="sm",
                                    color="primary",
                                    in_navbar=True,
                                    direction="left",
                                    caret=False,
                                )
                            ),
                        ],
                        className="g-2 ms-auto mt-3 mt-md-0",
                        align="end",
                    ),
                    id="navbar-collapse",
                    is_open=False,
                    navbar=True,
                ),
            ],
            fluid=True,
        ),
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
