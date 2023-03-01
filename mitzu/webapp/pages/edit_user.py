from __future__ import annotations

import flask
from typing import cast
from dash import (
    Input,
    Output,
    State,
    callback,
    html,
    register_page,
)
import dash_bootstrap_components as dbc
import dash.development.base_component as bc
import dash_mantine_components as dmc
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
import mitzu.webapp.service.user_service as US
from mitzu.webapp.helper import create_form_property_input
from mitzu.webapp.auth.decorator import (
    restricted_layout,
    restricted,
    restricted_for_admin,
)
from mitzu.webapp.webapp import MITZU_LOCATION

INDEX_TYPE = "user_property"
PROP_EMAIL = "email"
PROP_ROLE = "role"
PROP_PASSWORD = "password"
PROP_CONFIRM_PASSWORD = "confirm_password"


USER_SAVE_BUTTON = "user_save_button"
USER_CLOSE_BUTTON = "user_close_button"
NOT_FOUND_USER_CLOSE_BUTTON = "not_found_user_close_button"
USER_DELETE_BUTTON = "user_delete_button"
USER_CHANGE_PASSWORD_BUTTON = "user_change_password_button"
USER_CHANGE_ROLE_BUTTON = "user_change_role_button"

SAVE_RESPONSE_CONTAINER = "user_save_response_container"
DELETE_RESPONSE_CONTAINER = "user_delete_response_container"
CHANGE_PASSWORD_RESPONSE_CONTAINER = "user_change_password_response_container"
CHANGE_ROLE_RESPONSE_CONTAINER = "user_change_role_response_container"


@restricted_layout
def layout(user_id: str, **query_params) -> bc.Component:
    deps = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY))
    user_service = deps.user_service
    if user_service is None:
        raise ValueError("User service is not set")

    if deps.authorizer is None:
        raise ValueError("Authorizer is not set")

    logged_in_user_id = deps.authorizer.get_current_user_id()
    if logged_in_user_id is None:
        raise ValueError("Cannot determine logged in user id")
    logged_in_user = user_service.get_user_by_id(logged_in_user_id)
    if logged_in_user is None:
        raise ValueError("Logged in user is not found")

    is_admin = deps.authorizer.get_current_user_role(flask.request) == US.Role.ADMIN
    show_password_fields = user_id == "new"
    show_change_password = False
    show_delete_button = user_id != "new"
    if user_id is not None and user_id != "new":
        if user_id == "my-account":
            user_id = logged_in_user.id

        show_change_password = (
            user_id == logged_in_user.id or logged_in_user.role == US.Role.ADMIN
        )
        user = user_service.get_user_by_id(user_id)
        show_delete_button = (
            logged_in_user.role == US.Role.ADMIN
            and user is not None
            and user.id != logged_in_user.id
        )
    else:
        user = None

    if user is None and user_id != "new":
        return html.Div(
            [
                NB.create_mitzu_navbar("users_edit", []),
                dbc.Container(
                    [
                        html.H4("Users not found"),
                        html.Hr(),
                        dbc.Button(
                            [html.B(className="bi bi-x"), " Close"],
                            color="secondary",
                            class_name="me-3",
                            id=NOT_FOUND_USER_CLOSE_BUTTON,
                            href=P.USERS_PATH,
                        ),
                    ]
                ),
            ]
        )

    return html.Div(
        [
            NB.create_mitzu_navbar("users_management_page", []),
            dbc.Container(
                [
                    html.H4("User"),
                    html.Hr(),
                    create_form_property_input(
                        index_type=INDEX_TYPE,
                        property=PROP_EMAIL,
                        icon_cls="bi bi-envelope",
                        type="text",
                        required=True,
                        value=user.email if user is not None else "",
                        read_only=user is not None,
                    ),
                    create_form_property_input(
                        index_type=INDEX_TYPE,
                        property=PROP_ROLE,
                        icon_cls="bi bi-person-fill",
                        component_type=dmc.Select,
                        data=[
                            {"label": v.name.lower(), "value": v.value}
                            for v in US.Role.all_values()
                        ],
                        required=True,
                        value=user.role if user is not None else US.Role.MEMBER.value,
                        read_only=user_service.is_root_user(user_id),
                    ),
                    create_form_property_input(
                        index_type=INDEX_TYPE,
                        property=PROP_PASSWORD,
                        icon_cls="bi bi-key",
                        type="password",
                        required=True,
                        value="",
                    )
                    if show_password_fields
                    else None,
                    create_form_property_input(
                        index_type=INDEX_TYPE,
                        property=PROP_CONFIRM_PASSWORD,
                        icon_cls="bi bi-key",
                        type="password",
                        required=True,
                        value="",
                    )
                    if show_password_fields
                    else None,
                ]
                + (
                    [
                        html.Hr(),
                        html.Div(
                            [
                                dbc.Button(
                                    [
                                        html.B(className="bi bi-check-circle me-1"),
                                        "Save",
                                    ],
                                    color="success",
                                    class_name="me-3",
                                    id=USER_SAVE_BUTTON,
                                ),
                                dbc.Button(
                                    [html.B(className="bi bi-x me-1"), "Delete"],
                                    color="danger",
                                    class_name="me-3",
                                    id=USER_DELETE_BUTTON,
                                    external_link=True,
                                    href=P.create_path(P.USERS_PATH),
                                )
                                if show_delete_button
                                else None,
                            ],
                            className="mb-3",
                        ),
                        html.Div(
                            children=[], id=SAVE_RESPONSE_CONTAINER, className="lead"
                        ),
                        html.Div(
                            children=[], id=DELETE_RESPONSE_CONTAINER, className="lead"
                        ),
                    ]
                    if user is None
                    else (
                        [
                            html.Hr(),
                            html.Div(
                                [
                                    dbc.Button(
                                        ["Update"],
                                        color="primary",
                                        class_name="me-3",
                                        id=USER_CHANGE_ROLE_BUTTON,
                                        disabled=user_service.is_root_user(user_id),
                                    )
                                    if user is not None
                                    else None,
                                ],
                                className="mb-3",
                            ),
                            html.Div(
                                children=[],
                                id=CHANGE_ROLE_RESPONSE_CONTAINER,
                                className="lead",
                            ),
                        ]
                        if is_admin
                        else []
                    )
                )
                + (change_password_form() if show_change_password else [])
                + [
                    html.Hr(),
                    dbc.Button(
                        [html.B(className="bi bi-x me-1"), "Close"],
                        color="secondary",
                        class_name="me-3",
                        id=USER_CLOSE_BUTTON,
                        href=P.USERS_PATH,
                    ),
                ],
            ),
        ]
    )


def change_password_form():
    return [
        html.Hr(),
        create_form_property_input(
            index_type=INDEX_TYPE,
            property=PROP_PASSWORD,
            icon_cls="bi bi-key",
            type="password",
            required=False,
            value="",
        ),
        create_form_property_input(
            index_type=INDEX_TYPE,
            property=PROP_CONFIRM_PASSWORD,
            icon_cls="bi bi-key",
            type="password",
            required=False,
            value="",
        ),
        html.Hr(),
        dbc.Button(
            ["Change password"],
            color="primary",
            className="mb-3",
            id=USER_CHANGE_PASSWORD_BUTTON,
        ),
        html.Div(children=[], id=CHANGE_PASSWORD_RESPONSE_CONTAINER, className="lead"),
    ]


@callback(
    output={
        SAVE_RESPONSE_CONTAINER: Output(SAVE_RESPONSE_CONTAINER, "children"),
    },
    inputs={
        "n_clicks": Input(USER_SAVE_BUTTON, "n_clicks"),
    },
    state={
        "email": State({"type": INDEX_TYPE, "index": PROP_EMAIL}, "value"),
        "role": State({"type": INDEX_TYPE, "index": PROP_ROLE}, "value"),
        "password": State({"type": INDEX_TYPE, "index": PROP_PASSWORD}, "value"),
        "confirm_password": State(
            {"type": INDEX_TYPE, "index": PROP_CONFIRM_PASSWORD}, "value"
        ),
    },
    prevent_initial_call=True,
)
@restricted_for_admin
def create_new_user(n_clicks: int, email="", role="", password="", confirm_password=""):
    deps = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY))
    user_service = deps.user_service

    if user_service is None:
        raise ValueError("User service is not set")

    try:
        user_service.new_user(email, password, confirm_password, role=US.Role(role))
        return {
            SAVE_RESPONSE_CONTAINER: "User created!",
        }
    except Exception as e:
        return {
            SAVE_RESPONSE_CONTAINER: str(e),
        }


@callback(
    output={
        CHANGE_PASSWORD_RESPONSE_CONTAINER: Output(
            CHANGE_PASSWORD_RESPONSE_CONTAINER, "children"
        ),
    },
    inputs={
        "n_clicks": Input(USER_CHANGE_PASSWORD_BUTTON, "n_clicks"),
    },
    state={
        "password": State({"type": INDEX_TYPE, "index": PROP_PASSWORD}, "value"),
        "confirm_password": State(
            {"type": INDEX_TYPE, "index": PROP_CONFIRM_PASSWORD}, "value"
        ),
        "pathname": State(MITZU_LOCATION, "pathname"),
    },
    prevent_initial_call=True,
)
@restricted
def update_password(
    n_clicks: int, password="", confirm_password="", pathname: str = ""
):
    deps = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY))

    user_service = deps.user_service

    if user_service is None:
        raise ValueError("User service is not set")

    if deps.authorizer is None:
        raise ValueError("Authorizer is not set")

    try:
        logged_in_user_id = deps.authorizer.get_current_user_id()
        logged_in_user = user_service.get_user_by_id(logged_in_user_id)

        user_id = P.get_path_value(P.USERS_HOME_PATH, pathname, P.USER_PATH_PART)
        if logged_in_user is None:
            raise Exception("User is not signed in")

        if user_id == "my-account":
            user_id = logged_in_user_id

        if logged_in_user is None:
            raise Exception("User is not signed in")

        if logged_in_user.role != US.Role.ADMIN and logged_in_user.id != user_id:
            raise Exception("User is not authorized to change this password")

        user_service.update_password(user_id, password, confirm_password)
        return {CHANGE_PASSWORD_RESPONSE_CONTAINER: "Password changed"}
    except Exception as e:
        return {
            CHANGE_PASSWORD_RESPONSE_CONTAINER: str(e),
        }


@callback(
    output={
        CHANGE_ROLE_RESPONSE_CONTAINER: Output(
            CHANGE_ROLE_RESPONSE_CONTAINER, "children"
        ),
    },
    inputs={
        "n_clicks": Input(USER_CHANGE_ROLE_BUTTON, "n_clicks"),
    },
    state={
        "role": State({"type": INDEX_TYPE, "index": PROP_ROLE}, "value"),
        "pathname": State(MITZU_LOCATION, "pathname"),
    },
    prevent_initial_call=True,
)
@restricted_for_admin
def update_role(n_clicks: int, role="", pathname: str = ""):
    deps = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY))

    user_service = deps.user_service

    if user_service is None:
        raise ValueError("User service is not set")

    if deps.authorizer is None:
        raise ValueError("Authorizer is not set")

    try:
        logged_in_user_id = deps.authorizer.get_current_user_id()

        user_id = P.get_path_value(P.USERS_HOME_PATH, pathname, P.USER_PATH_PART)
        if logged_in_user_id is None:
            raise Exception("User is not signed in")

        if user_id == "my-account":
            user_id = logged_in_user_id

        user_service.update_role(user_id, US.Role(role))
        return {CHANGE_ROLE_RESPONSE_CONTAINER: "Role updated"}
    except Exception as e:
        return {
            CHANGE_ROLE_RESPONSE_CONTAINER: str(e),
        }


@callback(
    output={
        DELETE_RESPONSE_CONTAINER: Output(DELETE_RESPONSE_CONTAINER, "children"),
    },
    inputs={
        "n_clicks": Input(USER_DELETE_BUTTON, "n_clicks"),
    },
    state={
        "pathname": State(MITZU_LOCATION, "pathname"),
    },
    prevent_initial_call=True,
)
@restricted_for_admin
def delete_user(n_clicks: int, pathname: str = ""):
    user_id = P.get_path_value(P.USERS_HOME_PATH, pathname, P.USER_PATH_PART)
    deps = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY))
    user_service = deps.user_service

    if user_service is None:
        raise ValueError("User service is not set")

    try:
        if user_id == "my-account":
            raise Exception("Own account cannot be deleted")

        user_service.delete_user(user_id)
        return {
            DELETE_RESPONSE_CONTAINER: "User deleted",
        }

    except Exception as e:
        return {DELETE_RESPONSE_CONTAINER: str(e)}


register_page(
    __name__,
    path_template=P.USERS_HOME_PATH,
    title="Mitzu - Edit User",
    layout=layout,
)
