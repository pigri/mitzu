from dash import register_page
import dash_bootstrap_components as dbc
from dash import html
import mitzu.webapp.configs as configs
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted_layout


register_page(
    __name__,
    path=P.HOME_PATH,
    title="Mitzu",
)


@restricted_layout
def layout(**query_params):
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
                                    html.B(className="bi bi-play-circle me-1"),
                                    "Start exploring",
                                ],
                                color="secondary",
                                class_name="mb-3 w-25",
                                href=P.PROJECTS_PATH,
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
