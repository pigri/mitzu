from typing import List
from dash import (
    register_page,
    dcc,
    callback,
    Input,
    Output,
    State,
    html,
    no_update,
    ALL,
    callback_context,
)
import dash_bootstrap_components as dbc
import mitzu.webapp.configs as configs
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.paths as P
from mitzu.webapp.helper import create_form_property_input


INDEX_TYPE = "local_login"
INPUT_EMAIL = "email"
INPUT_PASSWORD = "password"
BUTTON_LOGIN = "login"
LOGIN_ERROR = "login_error"
LOCATION = "login_location"


def layout(**query_params):
    depenednecies = DEPS.Dependencies.get()
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
                dbc.Col(
                    login_with_oauth()
                    if depenednecies.authorizer._config.oauth
                    else login_with_local_users(),
                    class_name="mt-5",
                )
            ),
        ]
    )


def login_with_oauth() -> List:
    return [
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
                    size="sm",
                ),
            ],
            justify="center",
        ),
    ]


def login_with_local_users() -> List:
    return [
        dcc.Location(id=LOCATION, refresh=True),
        create_form_property_input(
            index_type=INDEX_TYPE,
            property=INPUT_EMAIL,
            icon_cls="bi bi-envelope",
            type="text",
            required=True,
            label_lg=1,
            input_lg=3,
            justify="center",
            name="email",
        ),
        create_form_property_input(
            index_type=INDEX_TYPE,
            property=INPUT_PASSWORD,
            icon_cls="bi bi-key",
            type="password",
            required=True,
            label_lg=1,
            input_lg=3,
            justify="center",
            name="password",
        ),
        dbc.Row(
            [
                dbc.Button(
                    [
                        "Login",
                    ],
                    color="secondary",
                    class_name="mb-3 w-25",
                    external_link=True,
                    type="submit",
                    name="login",
                    id=BUTTON_LOGIN,
                    size="sm",
                ),
            ],
            justify="center",
        ),
        dbc.Row(
            [
                dbc.Col(
                    [],
                    lg=3,
                    sm=3,
                    id=LOGIN_ERROR,
                ),
            ],
            justify="center",
        ),
    ]


@callback(
    [
        Output(LOCATION, "href"),
        Output(LOGIN_ERROR, "children"),
    ],
    Input(BUTTON_LOGIN, "n_clicks"),
    State({"type": INDEX_TYPE, "index": ALL}, "value"),
    prevent_initial_call=True,
)
def login(n_click: int, inputs: List[str]):
    authorizer = DEPS.Dependencies.get().authorizer

    redirect_when_authorized = authorizer.login_local_user(
        inputs[0], inputs[1], callback_context.response
    )
    if redirect_when_authorized:
        return (redirect_when_authorized, "")
    return (
        no_update,
        html.P(
            "Bad credentials",
            className="text-danger lead text-center",
        ),
    )


register_page(__name__, path=P.UNAUTHORIZED_URL, title="Mitzu", layout=layout)
