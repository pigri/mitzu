from __future__ import annotations

import dash.development.base_component as bc

import dash_bootstrap_components as dbc
from dash import html
from typing import Optional, cast, Callable, List, Tuple
import mitzu.webapp.pages.paths as P
import flask
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S


OFF_CANVAS_TOGGLER = "off-canvas-toggler"
EXPLORE_DROPDOWN = "explore-dropdown"
SIGNED_IN_AS_DIV = "signed-in-as"


class NavbarService:
    def __init__(self):
        self._left_navbar_providers: List[Tuple[int, Callable]] = []
        self._right_navbar_providers: List[Tuple[int, Callable]] = []

        self._init_default_navbars()

    def _init_default_navbars(self):
        def off_canvas_toggle(
            id: str, off_canvas_toggler_visible: bool = True, **kwargs
        ) -> Optional[bc.Component]:
            return dbc.Button(
                html.B(className="bi bi-list"),
                color="primary",
                size="sm",
                className="me-3",
                id={"type": OFF_CANVAS_TOGGLER, "index": id},
                style={
                    "display": "inline-block" if off_canvas_toggler_visible else "none"
                },
            )

        def explore_button(
            id: str,
            create_explore_button: bool = True,
            project_name: Optional[str] = None,
            storage: Optional[S.MitzuStorage] = None,
            **kwargs,
        ) -> Optional[bc.Component]:
            if create_explore_button:
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
                            href=P.create_path(
                                P.PROJECTS_EXPLORE_PATH, project_id=p.id
                            ),
                        )
                        for p in projects
                    ],
                    size="sm",
                    color="light",
                    label="explore" if project_name is None else project_name,
                    class_name="d-inline-block",
                    id={"type": EXPLORE_DROPDOWN, "index": id},
                )
            return None

        self._left_navbar_providers = [
            (0, off_canvas_toggle),
            (10, explore_button),
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
                className="text-light fw-bold",
                id={"type": SIGNED_IN_AS_DIV, "index": id},
            )

        self._right_navbar_providers = [
            (50, signed_in_as),
        ]

    def register_navbar_item_provider(self, menu_name: str, callback, priority=100):
        if menu_name == "left":
            self._left_navbar_providers.append((priority, callback))
            self._left_navbar_providers.sort(key=lambda x: x[0])
        elif menu_name == "right":
            self._right_navbar_providers.append((priority, callback))
            self._right_navbar_providers.sort(key=lambda x: x[0])
        else:
            raise ValueError(f"Unknow navbar menu: {menu_name}")

    def get_navbar_component(self, id: str, **kwargs) -> dbc.Navbar:
        left_navbar_comps = []
        right_navbar_comps = []

        for _, provider in self._left_navbar_providers:
            comp = provider(id, **kwargs)
            if comp is not None:
                left_navbar_comps.append(comp)

        for _, provider in self._right_navbar_providers:
            comp = provider(id, **kwargs)
            if comp is not None:
                right_navbar_comps.append(comp)

        res = dbc.Navbar(
            dbc.Container(
                [
                    dbc.Row(
                        children=[
                            dbc.Col(comp, width="auto") for comp in left_navbar_comps
                        ],
                        className="g-2",
                    ),
                    dbc.Row(
                        children=[
                            dbc.Col(comp, width="auto") for comp in right_navbar_comps
                        ],
                    ),
                ],
                fluid=True,
            ),
            class_name="mb-3",
            color="dark",
        )

        return res
