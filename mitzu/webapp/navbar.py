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
) -> bc.Component:
    if storage is None:
        storage = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        ).storage

    project_ids = storage.list_projects()
    projects = [storage.get_project(p_id) for p_id in project_ids]
    return dbc.DropdownMenu(
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
    )


LEFT_NAVBAR_ITEM_PROVIDERS: List[bc.Component] = []
RIGHT_NAVBAR_ITEM_PROVIDERS: List[bc.Component] = []


def init_navbar_item_providers():
    def off_canvas_toggle(id: str, **kwargs) -> Optional[bc.Component]:
        off_canvas_toggler_visible = kwargs.get("off_canvas_toggler_visible", True)
        return dbc.Button(
            html.B(className="bi bi-list"),
            color="primary",
            size="sm",
            className="me-3",
            id={"type": OFF_CANVAS_TOGGLER, "index": id},
            style={"display": "inline-block" if off_canvas_toggler_visible else "none"},
        )

    def explore_button(id: str, **kwargs) -> Optional[bc.Component]:
        create_explore_button = kwargs.get("create_explore_button", True)
        storage = kwargs.get("storage", None)
        project_name = kwargs.get("project_name", None)
        if create_explore_button:
            return create_explore_button_col(storage, project_name)
        return None

    global LEFT_NAVBAR_ITEM_PROVIDERS
    LEFT_NAVBAR_ITEM_PROVIDERS = [
        off_canvas_toggle,
        explore_button,
    ]

    def signed_in_as(id: str, **kwargs) -> Optional[bc.Component]:
        authorizer = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        ).authorizer

        storage = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        ).storage

        if authorizer is None:
            return None

        user_id = authorizer.get_current_user_id()
        if user_id is None:
            return None

        email = None
        if storage:
            user = storage.get_user_by_id(user_id)
            if user:
                email = user.email
        if email is None:
            email = user_id

        return html.Div(
            "Signed in as " + email,
            style={"color": "white", "line-height": "2.4em", "font-weight": "bold"},
        )

    global RIGHT_NAVBAR_ITEM_PROVIDERS
    RIGHT_NAVBAR_ITEM_PROVIDERS = [
        signed_in_as,
    ]


def create_mitzu_navbar(
    id: str,
    children: List[bc.Component] = [],
    **kwargs,
) -> dbc.Navbar:
    navbar_comps = []

    for provider in LEFT_NAVBAR_ITEM_PROVIDERS:
        comp = provider(id, **kwargs)
        if comp is not None:
            navbar_comps.append(comp)

    for comp in children:
        navbar_comps.append(comp)

    for provider in RIGHT_NAVBAR_ITEM_PROVIDERS:
        comp = provider(id, **kwargs)
        if comp is not None:
            navbar_comps.append(comp)

    res = dbc.Navbar(
        dbc.Container(
            [
                dbc.Row(
                    children=[dbc.Col(comp, width="auto") for comp in navbar_comps],
                    className="g-2",
                ),
            ],
            fluid=True,
        ),
        class_name="mb-3",
        color="dark",
    )

    return res
