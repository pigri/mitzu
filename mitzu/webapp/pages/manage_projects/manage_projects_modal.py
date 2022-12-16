from __future__ import annotations


import dash_bootstrap_components as dbc

import mitzu.webapp.dependencies as DEPS
from dash import html, dcc
import os
import flask
from typing import cast

CREATE_PROJECT_LINK = os.getenv(
    "CREATE_PROJECT_LINK", "https://github.com/mitzu-io/mitzu/blob/main/DOCS.md"
)

MANAGE_PROJECTS_MODAL = "manage-projects-modal"
MANAGE_PROJECTS_CLOSE = "manage-project-close"
UPLOAD_PROJECT_FILE = "upload-project-file"
DELETE_PROJECT_INDEX_TYPE = "delete-project-button"


def create_project_group_item(
    project_name: str, active: bool = False
) -> dbc.ListGroupItem:
    return dbc.ListGroupItem(
        [
            html.B("Sample project", className="lead"),
            dbc.Button(
                html.B(className="bi bi-x-lg"),
                id={
                    "type": DELETE_PROJECT_INDEX_TYPE,
                    "index": project_name,
                },
                size="sm",
                color="danger",
                outline=True,
                className="d-none" if active else "",
            ),
        ],
        className="d-flex justify-content-between align-items-center",
    )


def create_manage_projects_modal() -> dbc.Modal:
    dependencies: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    project_names = dependencies.storage.list_projects()

    res = dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Manage projects"), close_button=False),
            dbc.ModalBody(
                [
                    dbc.ListGroup(
                        [create_project_group_item(p, False) for p in project_names],
                        className="mb-3",
                        flush=True,
                    ),
                    html.Div(
                        [
                            dcc.Upload(
                                html.A("Upload Discovered Project"),
                                id=UPLOAD_PROJECT_FILE,
                                className="mb-3 btn btn-info",
                            ),
                            dbc.Button(
                                html.B(className="bi bi-info-circle"),
                                id="xxxx",
                                className="mb-3",
                                n_clicks=0,
                                color="secondary",
                                title="How to create and discover projects?",
                                href=CREATE_PROJECT_LINK,
                                external_link=True,
                            ),
                        ],
                        className="d-flex justify-content-between",
                    ),
                ]
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        "Close",
                        id=MANAGE_PROJECTS_CLOSE,
                        className="ms-auto",
                        n_clicks=0,
                        color="secondary",
                    ),
                ]
            ),
        ],
        id=MANAGE_PROJECTS_MODAL,
        is_open=False,
    )

    return res
