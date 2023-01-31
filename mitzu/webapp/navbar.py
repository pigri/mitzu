from __future__ import annotations

import dash.development.base_component as bc

import dash_bootstrap_components as dbc
from dash import html
from typing import List, Optional, cast
import mitzu.webapp.storage as S
import mitzu.webapp.pages.paths as P
import flask
import mitzu.webapp.dependencies as DEPS


OFF_CANVAS_TOGGLER = "off-canvas-toggler"


def create_explore_button_col(
    storage: Optional[S.MitzuStorage] = None, project_name: Optional[str] = None
):
    if storage is None:
        storage = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        ).storage

    project_ids = storage.list_projects()
    projects = [storage.get_project(p_id) for p_id in project_ids]
    return dbc.Col(
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem(
                    children=p.project_name,
                    href=P.create_path(P.PROJECTS_EXPLORE_PATH, project_id=p.id),
                )
                for p in projects
            ],
            size="sm",
            color="light",
            label="explore" if project_name is None else project_name,
            class_name="d-inline-block",
        ),
        width="auto",
    )


def create_mitzu_navbar(
    id: str,
    children: List[bc.Component] = [],
    storage: Optional[S.MitzuStorage] = None,
    create_explore_button: bool = True,
    off_canvas_toggler_visible: bool = True,
    project_name: Optional[str] = None,
) -> dbc.Navbar:

    navbar_children = [
        dbc.Col(
            dbc.Button(
                html.B(className="bi bi-list"),
                color="primary",
                size="sm",
                className="me-3",
                id={"type": OFF_CANVAS_TOGGLER, "index": id},
                style={
                    "display": "inline-block" if off_canvas_toggler_visible else "none"
                },
            ),
            width="auto",
        ),
    ]
    if create_explore_button:
        navbar_children.append(create_explore_button_col(storage, project_name))

    navbar_children.extend([dbc.Col(comp) for comp in children])
    res = dbc.Navbar(
        dbc.Container(
            [
                dbc.Row(
                    children=navbar_children,
                    className="g-2",
                ),
            ],
            fluid=True,
        ),
        class_name="mb-3",
        color="dark",
    )

    return res
