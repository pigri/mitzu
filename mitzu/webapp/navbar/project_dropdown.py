from __future__ import annotations

import os
from urllib.parse import urlparse

import dash_bootstrap_components as dbc
import mitzu.webapp.webapp as WA
from dash import Input, Output
from mitzu.webapp.helper import get_path_project_name, value_to_label

URL_BASE_PATHNAME = os.getenv("URL_BASE_PATHNAME")
CHOOSE_PROJECT_DROPDOWN = "choose-project-dropdown"


def get_project_path(project_name: str) -> str:
    if URL_BASE_PATHNAME is not None:
        return f"{URL_BASE_PATHNAME}/{project_name}"
    return f"/{project_name}"


def create_project_dropdown(webapp: WA.MitzuWebApp):
    projects = webapp.persistency_provider.list_projects()
    projects = [p.replace(".mitzu", "") for p in projects]
    res = (
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem(
                    value_to_label(p), href=get_project_path(p), external_link=True
                )
                for p in projects
            ],
            id=CHOOSE_PROJECT_DROPDOWN,
            in_navbar=True,
            label="Select project",
            size="sm",
            color="primary",
        ),
    )

    @webapp.app.callback(
        Output(CHOOSE_PROJECT_DROPDOWN, "label"),
        Input(WA.MITZU_LOCATION, "href"),
    )
    def update(href: str):
        parse_result = urlparse(href)
        print(parse_result)
        curr_path_project_name = get_path_project_name(parse_result)

        if not curr_path_project_name:
            return "Select project"
        return value_to_label(curr_path_project_name)

    return res
