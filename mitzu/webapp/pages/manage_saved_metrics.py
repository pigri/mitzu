import dash_bootstrap_components as dbc
from dash import (
    dcc,
    ALL,
    Input,
    Output,
    State,
    callback,
    ctx,
    html,
    register_page,
    no_update,
)
import dash.development.base_component as bc
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
import mitzu.webapp.storage as S
import mitzu.webapp.model as WM
from typing import List
import mitzu.webapp.dependencies as DEPS
from mitzu.webapp.auth.decorator import restricted, restricted_layout
import flask
from typing import cast
import datetime
from mitzu.webapp.helper import MISSING_RESOURCE_CSS
from urllib.parse import quote

CREATE_PROJECT_DOCS_LINK = "https://github.com/mitzu-io/mitzu/blob/main/DOCS.md"
SAVE_BUTTON = "project_save_button"
CLOSE_BUTTON = "project_close_button"
MANAGE_PROJECT_INFO = "manage_project_info"
SAVED_METRIC_DELETE = "save_metric_closed"
SAVED_METRICS_CONTAINER = "saved_metrics_container"
SAVED_METRICS_INFO = "saved_metrics_info"

CONFIRM_DIALOG_INDEX = "saved_metric_confirm"
CONFIRM_DIALOG_CLOSE = "saved_metric_confirm_dialog_close"
CONFIRM_DIALOG_ACCEPT = "saved_metric_confirm_dialog_accept"
CONFIRM_MODEL_BODY = "confirm_modal_body"
CONFIRM_METRIC_ID = "confirm_metric_id"

SEPARATOR = "###"


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
            html.Div(children="", className="d-none", id=CONFIRM_METRIC_ID),
        ],
        id=CONFIRM_DIALOG_INDEX,
        is_open=False,
    )


def create_saved_metric_card(
    saved_metric: WM.SavedMetric,
) -> bc.Component:
    metric = saved_metric.metric
    project = saved_metric.project

    if project is not None and metric is not None:
        # Copying metric without ID and name for further exploration
        none_saved_params = f"?m={quote(saved_metric.metric_json)}"
        none_saved_href = f"{P.create_path(P.PROJECTS_EXPLORE_PATH, project_id=project.id)}{none_saved_params}"

        # Creating href for edit button
        saved_params = f"?sm={saved_metric.id}"
        saved_href = f"{P.create_path(P.PROJECTS_EXPLORE_PATH, project_id=project.id)}{saved_params}"
    else:
        saved_href = ""
        none_saved_href = ""

    return dbc.Card(
        [
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Link(
                            children=[
                                dbc.CardImg(
                                    src=saved_metric.small_base64,
                                    className="img-fluid rounded-start",
                                    style={"width": "120px", "height": "100px"},
                                ),
                            ],
                            href=none_saved_href,
                        ),
                        className="col-md-4 ps-3",
                    ),
                    dbc.Col(
                        dbc.CardBody(
                            [
                                html.Div(
                                    [
                                        dcc.Link(
                                            html.H5(
                                                saved_metric.name,
                                                className="card-title",
                                            ),
                                            href=none_saved_href,
                                            className="text-dark",
                                        ),
                                        dbc.Button(
                                            html.I(className="bi bi-x-lg"),
                                            color="dark",
                                            id={
                                                "type": SAVED_METRIC_DELETE,
                                                "index": f"{saved_metric.id}{SEPARATOR}{saved_metric.name}",
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
                                            saved_metric.description,
                                            className="card-text",
                                        ),
                                        html.Small(
                                            saved_metric.created_at.strftime("%c"),
                                            className="card-text text-muted",
                                        ),
                                    ],
                                    href=none_saved_href,
                                    className="text-dark",
                                ),
                            ]
                        ),
                        className="col-md-8",
                    ),
                    dbc.Button(
                        [html.B(className="bi bi-gear me-1"), "Edit"],
                        size="sm",
                        color="light",
                        href=saved_href,
                        class_name="text-align-end",
                        disabled=project is None,
                    ),
                ],
                className="g-0 d-flex align-items-center",
            )
        ],
        class_name=MISSING_RESOURCE_CSS if project is None else "",
    )


def list_saved_metrics(storage: S.MitzuStorage) -> List[bc.Component]:
    saved_metric_ids = storage.list_saved_metrics()
    saved_metrics = [storage.get_saved_metric(s_id) for s_id in saved_metric_ids]
    saved_metrics.sort(key=lambda m: -datetime.datetime.timestamp(m.created_at))
    if len(saved_metrics) == 0:
        return dbc.Col(
            [
                html.H4("You don't have any saved metrics yet."),
                dbc.Button(
                    "Explore projects",
                    href=P.PROJECTS_PATH,
                    size="sm",
                    class_name="mt-3",
                ),
            ]
        )

    return [
        dbc.Col(
            create_saved_metric_card(sm),
            class_name="mb-3",
            sm=12,
            md=6,
            xl=4,
            lg=5,
            xs=12,
        )
        for sm in saved_metrics
    ]


@restricted_layout
def layout(**query_params) -> bc.Component:
    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage

    return html.Div(
        [
            NB.create_mitzu_navbar("saved_metrics_navbar", [], storage),
            dbc.Container(
                [
                    html.H4("Saved metrics"),
                    html.Hr(),
                    html.Div("", id=SAVED_METRICS_INFO, className="lead"),
                    dbc.Row(
                        list_saved_metrics(storage),
                        id=SAVED_METRICS_CONTAINER,
                    ),
                    create_confirm_dialog(),
                ]
            ),
        ]
    )


register_page(
    __name__,
    path=P.SAVED_METRICS,
    title="Mitzu - Saved Metrics",
    layout=layout,
)


@callback(
    Output(CONFIRM_DIALOG_INDEX, "is_open"),
    Output(CONFIRM_METRIC_ID, "children"),
    Output(CONFIRM_MODEL_BODY, "children"),
    Input({"type": SAVED_METRIC_DELETE, "index": ALL}, "n_clicks"),
    Input(CONFIRM_DIALOG_CLOSE, "n_clicks"),
    Input(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    prevent_initial_call=True,
)
@restricted
def delete_button_clicked(delete_clicks: List[int], close: int, accept: int):
    if ctx.triggered_id is None or all([v is None for v in delete_clicks]):
        return no_update, no_update, no_update

    if ctx.triggered_id in [CONFIRM_DIALOG_CLOSE, CONFIRM_DIALOG_ACCEPT]:
        return (False, "", "")

    index = ctx.triggered_id.get("index", "")
    parts = index.split(SEPARATOR)

    if len(parts) == 2:
        metric_id, metric_name = tuple(parts)
        message = f"Do you really want to delete {metric_name}"
        return (True, metric_id, message)

    return (False, "", "")


@callback(
    Output(SAVED_METRICS_CONTAINER, "children"),
    Input(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    State(CONFIRM_METRIC_ID, "children"),
    background=True,
    running=[
        (
            Output(SAVED_METRICS_INFO, "children"),
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
def confirm_button_clicked(close_button: int, metric_id: str) -> List:
    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage
    if ctx.triggered_id is not None:
        storage.clear_saved_metric(metric_id)

    return list_saved_metrics(storage)
