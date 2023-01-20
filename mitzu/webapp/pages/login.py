from dash import register_page
import dash_bootstrap_components as dbc
from dash import html
import mitzu.webapp.configs as configs
import mitzu.webapp.pages.paths as P


register_page(
    __name__,
    path=P.UNAUTHORIZED_URL,
    title="Mitzu",
)


def layout():
    return dbc.Container(
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
                            "Login",
                        ],
                        color="secondary",
                        class_name="mb-3 w-25",
                        href=P.REDIRECT_TO_LOGIN_URL,
                        external_link=True,
                    ),
                ],
                justify="center",
            ),
        ]
    )
