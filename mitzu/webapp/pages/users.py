from __future__ import annotations

import flask
from typing import cast
from dash import (
    html,
    register_page,
)
import dash_bootstrap_components as dbc
import dash.development.base_component as bc
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.service.user_service as user_service
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
from mitzu.webapp.helper import TBL_CLS, TBL_HEADER_CLS
from mitzu.webapp.auth.decorator import restricted_layout
import mitzu.webapp.service.user_service as US


ADD_USER_BUTTON = "user_add_user"
USERS_TABLE_ID = "users_table"
USERS_TBL_BODY = "users_tbl_body"

register_page(__name__, path=P.USERS_PATH, title="Mitzu - Users")


def create_users_table(user_service: user_service.UserService):
    table_header = html.Thead(
        html.Tr(
            [
                html.Th("User Email", className=TBL_HEADER_CLS + " fw-bold"),
                html.Th("Role", className=TBL_HEADER_CLS + " fw-bold"),
                html.Th("", className=TBL_HEADER_CLS),
            ],
        )
    )

    sorted_users = sorted(user_service.list_users(), key=lambda u: u.email)
    table_rows = []
    for user in sorted_users:
        table_rows.append(
            html.Tr(
                [
                    html.Td(user.email, className=TBL_CLS),
                    html.Td(user.role.value, className=TBL_CLS),
                    html.Td(
                        dbc.Button(
                            "Edit",
                            color="info",
                            href=P.create_path(P.USERS_HOME_PATH, user_id=user.id),
                            size="sm",
                            className=TBL_CLS,
                        ),
                    ),
                ],
            )
        )

    table_body = html.Tbody(table_rows, id=USERS_TBL_BODY)

    return dbc.Table(
        [table_header, table_body],
        hover=False,
        responsive=True,
        striped=True,
        size="sm",
        id=USERS_TABLE_ID,
    )


@restricted_layout
def layout(**query_params) -> bc.Component:
    deps = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY))
    user_service = deps.user_service
    if user_service is None:
        raise ValueError("User service is not set")

    authorizer = deps.authorizer
    if authorizer is None:
        raise ValueError("Authorizer is not set")

    is_admin = authorizer.get_current_user_role(flask.request) == US.Role.ADMIN

    table = create_users_table(user_service)

    return html.Div(
        [
            NB.create_mitzu_navbar("users_list", []),
            dbc.Container(
                [
                    html.H4("Registered Users"),
                    html.Hr(),
                    table,
                ]
                + (
                    [
                        html.Hr(),
                        dbc.Button(
                            [html.I(className="bi bi-plus-circle me-1"), "Add user"],
                            color="primary",
                            href=P.create_path(P.USERS_HOME_PATH, user_id="new"),
                            id=ADD_USER_BUTTON,
                        ),
                    ]
                    if is_admin
                    else []
                )
            ),
        ]
    )
