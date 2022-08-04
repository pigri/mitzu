from __future__ import annotations

import os

import dash_bootstrap_components as dbc
import mitzu.webapp.navbar.metric_type_handler as MNB
import mitzu.webapp.navbar.project_dropdown as PD
import mitzu.webapp.webapp as WA
from dash import html

LOGO = "/assets/logo.png"
MANAGE_PROJECTS_LINK = os.getenv("MANAGE_PROJECTS_LINK")


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
                            )
                        ),
                        dbc.Col(PD.create_project_dropdown(webapp)),
                        dbc.Col(
                            MNB.MetricTypeHandler.from_metric_type(
                                MNB.MetricType.SEGMENTATION
                            ).component
                        ),
                        dbc.Col(
                            dbc.DropdownMenu(
                                [
                                    dbc.DropdownMenuItem("Projects", header=True),
                                    dbc.DropdownMenuItem(
                                        "Manage projects",
                                        external_link=True,
                                        href=MANAGE_PROJECTS_LINK,
                                        disabled=(MANAGE_PROJECTS_LINK is None),
                                    ),
                                    dbc.DropdownMenuItem(divider=True),
                                    dbc.DropdownMenuItem("Sign out", disabled=True),
                                ],
                                label="More",
                                size="sm",
                                color="dark",
                                in_navbar=True,
                                align_end=True,
                                direction="down",
                            )
                        ),
                    ],
                ),
            ],
            fluid=True,
        ),
        sticky="top",
    )
    return res
