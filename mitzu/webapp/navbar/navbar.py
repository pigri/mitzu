from __future__ import annotations

import dash_bootstrap_components as dbc
import mitzu.webapp.navbar.metric_type_handler as MNB
import mitzu.webapp.navbar.project_dropdown as PD
import mitzu.webapp.webapp as WA
from dash import html

LOGO = "/assets/logo.png"


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
                            dbc.Col(
                                MNB.MetricTypeHandler.from_metric_type(
                                    MNB.MetricType.SEGMENTATION
                                ).component
                            ),
                        ]
                    ),
                ),
                dbc.Row(
                    children=[
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
    return res
