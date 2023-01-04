from __future__ import annotations

from typing import Any, Dict

import dash_bootstrap_components as dbc
import mitzu.webapp.navbar as NB
from dash import ctx, html, callback
from dash.dependencies import ALL, Input, Output
import mitzu.webapp.configs as configs

import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.authorizer import SIGN_OUT_URL

OFF_CANVAS = "off-canvas-id"
BUTTON_COLOR = "light"

CLOSE_BUTTON = "close-button"
SEARCH_INPUT = "search-input"

FAVORITE_QUERIES_BUTTON = "favorite_queries_button"
DASHBOARDS_BUTTON = "dashboards_button"

EVENTS_PROPERTIES_BUTTON = "events_and_properties_button"
PROJECTS_BUTTON = "projects_button"
CONNECTIONS_BUTTON = "connections_button"

USERS_BUTTON = "users_button"
SETTINGS_BUTTON = "settings_button"


MENU_ITEM_CSS = "mb-1 w-100 border-0 text-start"
EXPLORE_MENU_ITEM_CSS = "mb-1 w-100 text-start"


def create_offcanvas() -> dbc.Offcanvas:

    res = dbc.Offcanvas(
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        html.A(
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
                            className="border-0",
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
                [html.B(className="bi bi-star-fill me-1"), "Favorite queries"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.DASHBOARDS_PATH,
                id=FAVORITE_QUERIES_BUTTON,
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
                href=P.USER_PATH_PART,
                id=USERS_BUTTON,
            ),
            dbc.Button(
                [html.B(className="bi bi-gear me-1"), "Settings"],
                color=BUTTON_COLOR,
                class_name=MENU_ITEM_CSS,
                href=P.USER_PATH_PART,
                id=SETTINGS_BUTTON,
            ),
            html.Hr(className="mb-3"),
            dbc.Button(
                [html.B(className="bi bi-box-arrow-right me-1"), "Sign out"],
                color="light",
                class_name=MENU_ITEM_CSS,
                href=SIGN_OUT_URL,
                external_link=True,
            ),
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
                DASHBOARDS_BUTTON: Input(DASHBOARDS_BUTTON, "n_clicks"),
                PROJECTS_BUTTON: Input(PROJECTS_BUTTON, "n_clicks"),
                USERS_BUTTON: Input(USERS_BUTTON, "n_clicks"),
                CONNECTIONS_BUTTON: Input(CONNECTIONS_BUTTON, "n_clicks"),
            }
        },
        prevent_initial_call=True,
    )
    def off_canvas_toggled(items: Dict[str, Any]) -> bool:
        for off_c in ctx.args_grouping["items"][NB.OFF_CANVAS_TOGGLER]:
            if off_c.get("triggered", False) and off_c.get("value") is not None:
                return True

        return False

    return res
