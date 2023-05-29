import traceback
from typing import List

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
from dash import html, register_page

import mitzu.helper as H
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted_layout

CONNECTIONS_CONTAINER = "connections_container"
CONNECTION_TITLE = "connection_title"


@restricted_layout
def layout() -> bc.Component:
    depenednecies = DEPS.Dependencies.get()
    connection_ids = depenednecies.storage.list_connections()

    connections: List[M.Connection] = []
    for con_id in connection_ids:
        con = depenednecies.storage.get_connection(con_id)
        connections.append(con)

    return html.Div(
        [
            NB.create_mitzu_navbar("explore-navbar"),
            dbc.Container(
                children=[
                    dbc.Row(
                        [
                            dbc.Col(
                                html.H4("Manage connections", className="card-title"),
                                width="auto",
                            ),
                            dbc.Col(
                                dbc.Button(
                                    [
                                        html.B(className="bi bi-plus-circle me-1"),
                                        "New Connection",
                                    ],
                                    size="sm",
                                    color="primary",
                                    href=P.CONNECTIONS_CREATE_PATH,
                                ),
                                width="auto",
                                class_name="ms-auto",
                            ),
                        ],
                    ),
                    html.Hr(),
                    create_connections_container(connections),
                    html.Hr(),
                ]
            ),
        ]
    )


def create_connections_container(connections: List[M.Connection]):
    children = []

    for con in connections:
        try:
            comp = create_connection_selector(con)
            children.append(comp)
        except Exception as exc:
            traceback.print_exception(type(exc), exc, exc.__traceback__)

    if len(children) == 0:
        return html.H4(
            "You don't have any connections yet...", className="card-title text-center"
        )

    return dbc.Row(children=children, id=CONNECTIONS_CONTAINER)


def create_connection_selector(connection: M.Connection) -> bc.Component:
    details: List[str] = []
    if connection.host is not None:
        details.append(f"Host: {connection.host}")
    if connection.port is not None:
        details.append(f"Port: {connection.port}")
    if connection.catalog is not None:
        details.append(f"Catalog: {connection.catalog}")
    if connection.schema is not None:
        details.append(f"Schema: {connection.schema}")

    project_jumbotron = dbc.Col(
        dbc.Card(
            [
                dbc.CardHeader(
                    H.value_to_label(connection.connection_name),
                    class_name="lead",
                    id=CONNECTION_TITLE,
                ),
                dbc.CardBody(
                    html.Div(
                        children=[
                            html.Img(
                                src=f"/assets/warehouse/{str(connection.connection_type.name).lower()}.png",
                                height=40,
                            ),
                            *[html.Div(d) for d in details],
                        ],
                        style={"min-height": "100px"},
                    )
                ),
                dbc.CardFooter(
                    dbc.Button(
                        "Manage",
                        size="sm",
                        color="primary",
                        href=P.create_path(
                            P.CONNECTIONS_MANAGE_PATH,
                            connection_id=connection.id,
                        ),
                    ),
                ),
            ],
            class_name="mb-3",
        ),
        lg=3,
        sm=12,
    )
    return project_jumbotron


register_page(
    __name__,
    path=P.CONNECTIONS_PATH,
    title="Mitzu - Connections",
)
