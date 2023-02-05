from typing import Any, List, Optional, cast

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import flask
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
)

import mitzu.helper as H
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.connections.manage_connections_component as MCC
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted, restricted_layout
from mitzu.webapp.helper import MITZU_LOCATION

CONNECTION_SAVE_BUTTON = "connection_save_button"
CONNECTION_CLOSE_BUTTON = "connection_close_button"
SAVE_RESPONSE_CONTAINER = "save_response_container"


@restricted_layout
def no_connection_layout():
    return layout(None)


@restricted_layout
def layout(connection_id: Optional[str] = None, **query_params) -> bc.Component:
    connection: Optional[M.Connection] = None
    if connection_id is not None:
        depenednecies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        connection = depenednecies.storage.get_connection(connection_id)

    title = "Create new connection" if connection is None else "Manage connection"

    return dbc.Form(
        [
            NB.create_mitzu_navbar("create-connection-navbar", []),
            dbc.Container(
                children=[
                    html.H4(title),
                    html.Hr(),
                    MCC.create_manage_connection_component(connection),
                    html.Hr(),
                    html.Div(
                        [
                            dbc.Button(
                                [html.B(className="bi bi-x"), " Close"],
                                color="secondary",
                                class_name="me-3",
                                id=CONNECTION_CLOSE_BUTTON,
                                href=P.CONNECTIONS_PATH,
                            ),
                            dbc.Button(
                                [html.B(className="bi bi-check-circle"), " Save"],
                                color="success",
                                id=CONNECTION_SAVE_BUTTON,
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(children=[], id=SAVE_RESPONSE_CONTAINER),
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
    Output(SAVE_RESPONSE_CONTAINER, "children"),
    Input(CONNECTION_SAVE_BUTTON, "n_clicks"),
    State({"type": MCC.INDEX_TYPE, "index": ALL}, "value"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def save_button_clicked(
    n_clicks: int, values: List[Any], pathname: str
) -> List[bc.Component]:
    if n_clicks is None:
        return no_update

    vals = {}
    for prop in ctx.args_grouping[1]:
        id_val = prop["id"]
        if id_val.get("type") == MCC.INDEX_TYPE:
            vals[id_val.get("index")] = prop["value"]

    connection = MCC.create_connection_from_values(vals)
    depenednecies: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )

    invalid = MCC.validate_input_values(values=vals)
    if invalid is not None:
        return html.P(f"Invalid {H.value_to_label(invalid)}", className="lead")
    depenednecies.storage.set_connection(connection.id, connection)

    return [html.P("Connection saved", className="lead")]
