from typing import Any, List, Optional, Tuple, Dict

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
from dash import (
    ALL,
    Input,
    Output,
    State,
    callback,
    ctx,
    html,
    no_update,
    register_page,
    dcc,
)
from mitzu.webapp.helper import MITZU_LOCATION
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.connections.manage_connections_component as MCC
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted, restricted_layout


CONNECTION_SAVE_BUTTON = "connection_save_button"
CONNECTION_CLOSE_BUTTON = "connection_close_button"
CONNECTION_LOCATION = "connection_location"
SAVE_RESPONSE_CONTAINER = "save_response_container"

DELETE_CONNECTION_RESULT = "delete_connection_result"
CONFIRM_DIALOG_INDEX = "connection_confirm"
CONFIRM_DIALOG_CLOSE = "conn_confirm_dialog_close"
CONFIRM_DIALOG_ACCEPT = "conn_confirm_dialog_accept"
CONNECTION_DELETE_BUTTON = "connection_delete_button"

CONPAGE_INDEX_TYPE = "connections_page"
CONPAGE_TEST_LOADING_ID = "connections_page_test_loading"

CONPAGE_CC_CONFIG = MCC.ConnectionComponentConfig(
    index_type=CONPAGE_INDEX_TYPE, test_loading_id=CONPAGE_TEST_LOADING_ID
)


def create_confirm_dialog():
    return dbc.Modal(
        [
            dbc.ModalBody(
                "Do you really want to delete the connection?", class_name="lead"
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        "Close",
                        id=CONFIRM_DIALOG_CLOSE,
                        size="sm",
                        color="secondary",
                        class_name="me-1",
                    ),
                    dbc.Button(
                        "Delete",
                        id=CONFIRM_DIALOG_ACCEPT,
                        size="sm",
                        color="danger",
                        href=P.CONNECTIONS_PATH,
                        external_link=True,
                    ),
                ]
            ),
        ],
        id=CONFIRM_DIALOG_INDEX,
        is_open=False,
    )


def create_delete_button(connection: Optional[M.Connection]) -> bc.Component:
    if connection is not None:
        return dbc.Button(
            [html.B(className="bi bi-x-circle mr-1"), "Delete connection"],
            id=CONNECTION_DELETE_BUTTON,
            size="sm",
            color="danger",
        )
    else:
        return html.Div()


@restricted_layout
def no_connection_layout():
    return layout(None)


@restricted_layout
def layout(connection_id: Optional[str] = None, **query_params) -> bc.Component:
    connection: Optional[M.Connection] = None
    if connection_id is not None:
        depenednecies = DEPS.Dependencies.get()
        connection = depenednecies.storage.get_connection(connection_id)

    title = "Create new connection" if connection is None else "Manage connection"

    return dbc.Form(
        [
            NB.create_mitzu_navbar("create-connection-navbar"),
            dbc.Container(
                children=[
                    dcc.Location(id=CONNECTION_LOCATION),
                    html.H4(title),
                    html.Hr(),
                    MCC.create_manage_connection_component(
                        connection, cc_config=CONPAGE_CC_CONFIG
                    ),
                    html.Hr(),
                    html.Div(
                        [
                            dbc.Button(
                                [html.B(className="bi bi-x"), " Close"],
                                color="secondary",
                                class_name="me-3",
                                id=CONNECTION_CLOSE_BUTTON,
                                href=P.CONNECTIONS_PATH,
                                size="sm",
                            ),
                            dbc.Button(
                                [html.B(className="bi bi-check-circle"), " Save"],
                                color="success",
                                class_name="me-3",
                                id=CONNECTION_SAVE_BUTTON,
                                size="sm",
                            ),
                            create_delete_button(connection),
                        ],
                        className="mb-3",
                    ),
                    html.Div(children=[], id=DELETE_CONNECTION_RESULT),
                    html.Div(children=[], id=SAVE_RESPONSE_CONTAINER),
                    create_confirm_dialog(),
                ],
                class_name="mb-3",
            ),
        ]
    )


register_page(
    __name__ + "_create",
    path=P.CONNECTIONS_CREATE_PATH,
    title="Mitzu - Create Connection",
    layout=no_connection_layout,
)


register_page(
    __name__,
    path_template=P.CONNECTIONS_MANAGE_PATH,
    title="Mitzu - Manage Connection",
    layout=layout,
)


@callback(
    Output(
        {"type": CONPAGE_INDEX_TYPE, "index": MCC.EXTRA_PROPERTY_CONTAINER},
        "children",
    ),
    Input({"type": CONPAGE_INDEX_TYPE, "index": MCC.PROP_CONNECTION_TYPE}, "value"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def connection_type_changed_callback(
    connection_type: str, pathname: str
) -> List[bc.Component]:
    return MCC.connection_type_changed(
        connection_type=connection_type,
        pathname=pathname,
        cc_config=CONPAGE_CC_CONFIG,
    )


@callback(
    Output(
        {"type": CONPAGE_INDEX_TYPE, "index": MCC.TEST_CONNECTION_RESULT},
        "children",
    ),
    Input(
        {"type": CONPAGE_INDEX_TYPE, "index": MCC.CONNECTION_TEST_BUTTON},
        "n_clicks",
    ),
    State({"type": CONPAGE_INDEX_TYPE, "index": ALL}, "value"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
    background=True,
    running=[
        (
            Output(
                CONPAGE_TEST_LOADING_ID,
                "style",
            ),
            {"display": "inline-block"},
            {"display": "none"},
        ),
    ],
    interval=100,
)
@restricted
def test_connection_clicked_callback(
    n_clicks: int, values: Dict[str, Any], pathname: str
) -> List[bc.Component]:
    return MCC.test_connection_clicked(
        n_clicks=n_clicks,
        cc_config=CONPAGE_CC_CONFIG,
        args_groupping=ctx.args_grouping[1],
    )


@callback(
    Output(SAVE_RESPONSE_CONTAINER, "children"),
    Output(CONNECTION_LOCATION, "href"),
    Input(CONNECTION_SAVE_BUTTON, "n_clicks"),
    State({"type": CONPAGE_INDEX_TYPE, "index": ALL}, "value"),
    prevent_initial_call=True,
)
@restricted
def save_button_clicked(
    save_button_clicks: int,
    values: List[Any],
) -> List[bc.Component]:
    if save_button_clicks is None:
        return no_update

    try:
        connection = MCC.create_connection_from_inputs(
            CONPAGE_CC_CONFIG, ctx.args_grouping[1]
        )
        depenednecies = DEPS.Dependencies.get()
        depenednecies.storage.set_connection(connection.id, connection)
        depenednecies.tracking_service.track_connection_saved(connection)

        if ctx.triggered_id == CONNECTION_SAVE_BUTTON:
            return [html.P("Connection saved", className="lead"), no_update]
        else:
            return [
                no_update,
                f"{P.PROJECTS_CREATE_PATH}?connection_id={connection.id}",
            ]
    except Exception as exc:
        return [
            html.P(
                f"Saving failed: {str(exc)[:100]}",
                className="lead text-danger",
            ),
            no_update,
        ]


@callback(
    Output(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    Input(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def delete_confirmed_clicked(n_clicks: int, pathname: str) -> int:
    if n_clicks is None:
        return no_update
    deps = DEPS.Dependencies.get()
    connection_id = P.get_path_value(
        P.CONNECTIONS_MANAGE_PATH, pathname, P.CONNECTION_ID_PATH_PART
    )
    deps.storage.delete_connection(connection_id)
    return n_clicks


@callback(
    Output(CONFIRM_DIALOG_INDEX, "is_open"),
    Output(DELETE_CONNECTION_RESULT, "children"),
    Input(CONNECTION_DELETE_BUTTON, "n_clicks"),
    Input(CONFIRM_DIALOG_CLOSE, "n_clicks"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def delete_button_clicked(delete: int, close: int, pathname: str) -> Tuple[bool, str]:
    if delete is None:
        return no_update, no_update
    deps = DEPS.Dependencies.get()
    if ctx.triggered_id == CONNECTION_DELETE_BUTTON:
        connection_id = P.get_path_value(
            P.CONNECTIONS_MANAGE_PATH, pathname, P.CONNECTION_ID_PATH_PART
        )
        projects = deps.storage.list_projects()
        for project in projects:
            prj = deps.storage.get_project(project.id)
            if prj.get_connection_id() == connection_id:
                return False, html.Div(
                    children=[
                        html.Span(
                            "You can't delete this connection because it is used by  "
                        ),
                        dcc.Link(
                            prj.project_name,
                            P.create_path(
                                P.PROJECTS_MANAGE_PATH, project_id=project.id
                            ),
                        ),
                    ],
                    className="my-3 text-danger lead",
                )
        return True, ""

    return False, ""
