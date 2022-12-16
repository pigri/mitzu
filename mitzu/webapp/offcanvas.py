from __future__ import annotations

from typing import Dict, Any
import dash_bootstrap_components as dbc
import mitzu.webapp.navbar as NB
from dash import ctx, html, callback
from dash.dependencies import ALL, Input, Output
import mitzu.webapp.configs as configs

OFF_CANVAS = "off-canvas-id"
SEARCH_INPUT = "search-input"
CLOSE_BUTTON = "close-button"
MANAGE_PROJECTS_BUTTON = "manage-projects"
DASHBOARDS_BUTTON = "dashboards-button"
EXPLORE_BUTTON = "explore-button"
MENU_ITEM_CSS = "mb-3 w-100 border-0 text-start"


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
                            )
                        )
                    ),
                    dbc.Col(
                        dbc.Button(
                            html.I(className="bi bi-x-lg"),
                            color="secondary",
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
                [html.B(className="bi bi-play-circle"), " Explore"],
                color="secondary",
                class_name=MENU_ITEM_CSS,
                href="/explore",
                id=EXPLORE_BUTTON,
            ),
            dbc.Button(
                [html.B(className="bi bi-columns-gap"), " Dashboards"],
                color="secondary",
                class_name=MENU_ITEM_CSS,
                href="/dashboards",
                id=DASHBOARDS_BUTTON,
            ),
            html.Hr(className="mb-3"),
            dbc.Button(
                [html.B(className="bi bi-gear"), " Manage projects"],
                color="secondary",
                class_name=MENU_ITEM_CSS,
                href="/projects",
                id=MANAGE_PROJECTS_BUTTON,
            ),
            html.Hr(className="mb-3"),
            dbc.Button(
                "Sign out",
                color="secondary",
                class_name=f"{MENU_ITEM_CSS} {'d-none' if not configs.SIGN_OUT_URL else ''}",
                href=configs.SIGN_OUT_URL,
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
                MANAGE_PROJECTS_BUTTON: Input(MANAGE_PROJECTS_BUTTON, "n_clicks"),
                DASHBOARDS_BUTTON: Input(DASHBOARDS_BUTTON, "n_clicks"),
                EXPLORE_BUTTON: Input(EXPLORE_BUTTON, "n_clicks"),
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
