from dash import register_page
import dash_bootstrap_components as dbc
from dash import html
import mitzu.webapp.configs as configs
import mitzu.webapp.navbar as NB

register_page(
    __name__,
    path="/",
    title="Mitzu",
)

START_EXPLORING = "start_exploring"


def layout():
    return html.Div(
        [
            NB.create_mitzu_navbar("home-navbar", []),
            dbc.Container(
                children=[
                    dbc.Row(
                        [
                            html.Img(
                                src=configs.DASH_LOGO_PATH,
                                height="100px",
                                className="logo",
                            )
                        ],
                        justify="center",
                    ),
                    html.Hr(),
                    dbc.Row(
                        [
                            dbc.Button(
                                [
                                    html.B(className="bi bi-play-circle"),
                                    "Start exploring",
                                ],
                                color="secondary",
                                class_name="mb-3 w-25 ",
                                href="/explore/",
                                id=START_EXPLORING,
                            ),
                        ],
                        justify="center",
                    ),
                    html.Hr(),
                    dbc.Row(
                        [],
                        justify="center",
                    ),
                ]
            ),
        ]
    )
