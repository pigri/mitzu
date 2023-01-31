import traceback
from typing import List

import dash.development.base_component as bc
import dash_bootstrap_components as dbc

from dash import html, register_page, callback, Input, Output, ctx, State, ALL, dcc

import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.model as WM
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted, restricted_layout
from typing import cast
import flask
from dash_iconify import DashIconify

register_page(__name__, path_template=P.DASHBOARDS_PATH, title="Mitzu - Dashboards")


CREATE_NEW_DASHBOARD_BTN = "create_new_dashboard"
DASHBOARD_DELETE = "dashboard_delete"

SEPARATOR = "###"

CONFIRM_DIALOG_MODEL = "dashboard_delete_confirm"
CONFIRM_DIALOG_CLOSE = "dashboard_delete_confirm_dialog_close"
CONFIRM_DIALOG_ACCEPT = "dashboard_delete_confirm_dialog_accept"
CONFIRM_MODEL_BODY = "dashboard_confirm_modal_body"
CONFIRM_DASHBOARD_ID = "dashboard_confirm_id"

DASHBOARD_CONTAINER = "dashboard_container"
DASHBOARDS_INFO = "dashboard_info"


@restricted_layout
def layout(**query_params) -> bc.Component:
    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage
    d_ids = storage.list_dashboards()
    dashboards = [storage.get_dashboard(d_id) for d_id in d_ids]

    return html.Div(
        [
            NB.create_mitzu_navbar("dashboard-navbar", []),
            dbc.Container(
                children=[
                    dbc.Row(
                        [
                            dbc.Col(
                                html.H4("Select a Dashboard", className="card-title"),
                                width="auto",
                            ),
                            dbc.Col(
                                dbc.Button(
                                    [
                                        html.B(
                                            className="bi bi-plus-circle me-1",
                                        ),
                                        "Add Dashboard",
                                    ],
                                    color="primary",
                                    href=P.DASHBOARDS_CREATE_PATH,
                                    id=CREATE_NEW_DASHBOARD_BTN,
                                ),
                                width="auto",
                                class_name="ms-auto",
                            ),
                        ]
                    ),
                    html.Hr(),
                    html.Div(children="", id=DASHBOARDS_INFO),
                    dbc.Row(
                        children=list_dashboards(dashboards),
                        id=DASHBOARD_CONTAINER,
                    ),
                    create_confirm_dialog(),
                ]
            ),
        ]
    )


def create_confirm_dialog():
    return dbc.Modal(
        [
            dbc.ModalBody(
                "Do you really want to delete this saved metric?",
                class_name="lead",
                id=CONFIRM_MODEL_BODY,
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
                    ),
                ]
            ),
            html.Div(children="", className="d-none", id=CONFIRM_DASHBOARD_ID),
        ],
        id=CONFIRM_DIALOG_MODEL,
        is_open=False,
    )


def list_dashboards(dashboards: List[WM.Dashboard]) -> List[bc.Component]:
    children = []

    for dashboard in dashboards:
        try:
            comp = create_dashboard_selector(dashboard)
            children.append(dbc.Col(comp, sm=12, lg=4, class_name="mb-3"))
        except Exception as exc:
            traceback.print_exception(exc)

    if len(children) == 0:
        return html.H4(
            "You don't have any dashboards yet",
            className="card-title text-center",
        )

    return children


def create_dashboard_selector(dashboard: WM.Dashboard) -> bc.Component:
    details: List[str] = []
    if dashboard.created_at is not None:
        details.append(f"Created at: {dashboard.created_at}")
    if dashboard.last_updated_at is not None:
        details.append(f"Edited at: {dashboard.last_updated_at}")

    href = P.create_path(P.DASHBOARDS_EDIT_PATH, dashboard_id=dashboard.id)

    return dbc.Card(
        [
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Link(
                            DashIconify(
                                icon="ic:twotone-dashboard",
                                width=100,
                                height=100,
                                color="gray",
                            ),
                            style={"width": "1rem", "height": "1rem"},
                            href=href,
                        ),
                        className="col-md-4 ps-3",
                    ),
                    dbc.Col(
                        dbc.CardBody(
                            [
                                html.Div(
                                    [
                                        dcc.Link(
                                            html.H6(
                                                dashboard.name,
                                                className="card-title",
                                            ),
                                            href=href,
                                            className="text-dark",
                                        ),
                                        dbc.Button(
                                            html.I(className="bi bi-x-lg"),
                                            color="dark",
                                            id={
                                                "type": DASHBOARD_DELETE,
                                                "index": f"{dashboard.id}{SEPARATOR}{dashboard.name}",
                                            },
                                            size="sm",
                                            outline=True,
                                            className="border-0 align-top",
                                        ),
                                    ],
                                    className="d-flex justify-content-between",
                                ),
                                dcc.Link(
                                    children=[
                                        html.P(
                                            # TODO add description
                                            "",
                                            className="card-text text-nowrap    ",
                                        ),
                                        html.Small(
                                            f"Created on {dashboard.created_at.strftime('%c')}",
                                            className="card-text text-muted",
                                        ),
                                    ],
                                    href=href,
                                    className="text-dark",
                                ),
                            ]
                        ),
                        className="col-md-8",
                    ),
                ],
                className="g-0 d-flex align-items-center",
            )
        ],
    )


@callback(
    Output(CONFIRM_DIALOG_MODEL, "is_open"),
    Output(CONFIRM_DASHBOARD_ID, "children"),
    Output(CONFIRM_MODEL_BODY, "children"),
    Input({"type": DASHBOARD_DELETE, "index": ALL}, "n_clicks"),
    Input(CONFIRM_DIALOG_CLOSE, "n_clicks"),
    Input(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    prevent_initial_call=True,
)
@restricted
def delete_button_clicked(delete: int, close: int, accept: int):
    if (
        ctx.triggered_id is None
        or ctx.triggered_id
        in [
            CONFIRM_DIALOG_CLOSE,
            CONFIRM_DIALOG_ACCEPT,
        ]
        or all(d is None for d in delete)
    ):
        return (False, "", "")

    index = ctx.triggered_id.get("index", "")
    parts = index.split(SEPARATOR)
    if len(parts) == 2:
        dashboard_id, name = tuple(parts)
        message = f"Do you really want to delete {name}"
        return (True, dashboard_id, message)

    return (False, "", "")


@callback(
    Output(DASHBOARD_CONTAINER, "children"),
    Input(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    State(CONFIRM_DASHBOARD_ID, "children"),
    background=True,
    running=[
        (
            Output(DASHBOARDS_INFO, "children"),
            [
                dbc.Spinner(
                    spinner_style={"width": "1rem", "height": "1rem"},
                    spinner_class_name="me-1",
                ),
                "Loading saved metrics",
            ],
            "",
        )
    ],
    interval=200,
    prevent_initial_call=True,
)
@restricted
def confirm_button_clicked(accept_button: int, dashboard_id: str) -> List:
    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage
    if ctx.triggered_id is not None:
        storage.clear_dashboard(dashboard_id)

    d_ids = storage.list_dashboards()
    dashboards = [storage.get_dashboard(d_id) for d_id in d_ids]
    return list_dashboards(dashboards)
