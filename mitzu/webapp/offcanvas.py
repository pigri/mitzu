from __future__ import annotations


from typing import Any, Dict

import dash_bootstrap_components as dbc
from dash import callback, ctx, html, dcc
from dash.dependencies import ALL, Input, Output

import mitzu.webapp.configs as configs
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted

OFF_CANVAS = "off-canvas-id"
BUTTON_COLOR = "light"

CLOSE_BUTTON = "close-button"
SEARCH_INPUT = "search-input"

EXPLORE_BUTTON = "explore_button"
SAVED_METRICS_BUTTON = "saved_metrics"
DASHBOARDS_BUTTON = "dashboards_button"

EVENTS_PROPERTIES_BUTTON = "events_and_properties_button"
PROJECTS_BUTTON = "projects_button"
CONNECTIONS_BUTTON = "connections_button"

USERS_BUTTON = "users_button"
MY_ACCOUNT_BUTTON = "my_account_button"


MENU_ITEM_CSS = "mb-1 w-100 border-0 text-start"
EXPLORE_MENU_ITEM_CSS = "mb-1 w-100 text-start"


def create_offcanvas(dependencies: DEPS.Dependencies) -> dbc.Offcanvas:
    project_ids = dependencies.storage.list_projects()
    project_id = project_ids[0] if len(project_ids) > 0 else None

    show_users = False
    show_sign_out = False
    if dependencies.authorizer is not None:
        show_sign_out = True
        show_users = dependencies.authorizer._config.oauth is None

    res = dbc.Offcanvas(
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Link(
                            html.Img(
                                src=configs.DASH_LOGO_PATH,
                                height="40px",
                                className="logo",
                            ),
                            href="/",
                        )
                    ),
                    dbc.Col(
                        dbc.Button(
                            html.I(className="bi bi-x-lg"),
                            color="dark",
                            id=CLOSE_BUTTON,
                            size="sm",
                            outline=True,
                            className="border-0 card-title pt-0",
                        ),
                        className="text-end",
                    ),
                ],
                className="mb-3",
            ),
            dbc.Input(
                id=SEARCH_INPUT,
                placeholder="Search ...",
                type="search",
                size="sm",
                className="mb-3 shadow-none",
            ),
            html.Hr(className="mb-3"),
            dbc.Button(
                [html.B(className="bi bi-search me-1"), "Explore"],
                color="info",
                class_name=MENU_ITEM_CSS,
                href=(
                    P.create_path(P.PROJECTS_EXPLORE_PATH, project_id=project_id)
                    if project_id is not None
                    else P.PROJECTS_PATH
                ),
                id=EXPLORE_BUTTON,
            ),
            dbc.Button(
                [html.B(className="bi bi-star-fill me-1"), "Saved metrics"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.SAVED_METRICS,
                id=SAVED_METRICS_BUTTON,
            ),
            dbc.Button(
                [html.B(className="bi bi-columns-gap me-1"), "Dashboards"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.DASHBOARDS_PATH,
                id=DASHBOARDS_BUTTON,
            ),
            html.Hr(className="mb-3"),
            dbc.Button(
                [html.B(className="bi bi-card-list me-1"), "Events and properties"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.EVENTS_AND_PROPERTIES_PATH,
                id=EVENTS_PROPERTIES_BUTTON,
            ),
            dbc.Button(
                [html.B(className="bi bi-play-circle me-1"), "Projects"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.PROJECTS_PATH,
                id=PROJECTS_BUTTON,
            ),
            dbc.Button(
                [html.B(className="bi bi-plugin me-1"), "Connections"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.CONNECTIONS_PATH,
                id=CONNECTIONS_BUTTON,
            ),
            html.Hr(className="mb-3"),
            dbc.Button(
                [html.B(className="bi bi-person-circle me-1"), "Users"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.USERS_PATH,
                id=USERS_BUTTON,
            )
            if show_users
            else None,
            dbc.Button(
                [html.B(className="bi bi-person-circle me-1"), "My Account"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.create_path(P.USERS_HOME_PATH, user_id="my-account"),
                id=MY_ACCOUNT_BUTTON,
            )
            if show_users
            else None,
            html.Hr(className="mb-3 d-none"),
            dbc.Button(
                [html.B(className="bi bi-box-arrow-right me-1"), "Sign out"],
                color="light",
                class_name=MENU_ITEM_CSS,
                href=P.SIGN_OUT_URL,
                external_link=True,
            )
            if show_sign_out
            else None,
        ],
        close_button=False,
        is_open=False,
        id=OFF_CANVAS,
    )

    @callback(
        Output(OFF_CANVAS, "is_open"),
        inputs={
            "items": {
                NB.OFF_CANVAS_TOGGLER: Input(
                    {"type": NB.OFF_CANVAS_TOGGLER, "index": ALL}, "n_clicks"
                ),
                CLOSE_BUTTON: Input(CLOSE_BUTTON, "n_clicks"),
            }
        },
        prevent_initial_call=True,
    )
    @restricted
    def off_canvas_toggled(items: Dict[str, Any]) -> bool:
        for off_c in ctx.args_grouping["items"][NB.OFF_CANVAS_TOGGLER]:
            if off_c.get("triggered", False) and off_c.get("value") is not None:
                return True

        return False

    return res
