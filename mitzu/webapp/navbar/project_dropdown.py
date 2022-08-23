from __future__ import annotations

from urllib.parse import urlparse

import dash_bootstrap_components as dbc
import mitzu.webapp.webapp as WA
from dash import Input, Output
from mitzu.webapp.helper import get_path_project_name, value_to_label

CHOOSE_PROJECT_DROPDOWN = "choose-project-dropdown"


def create_project_dropdown(webapp: WA.MitzuWebApp):
    projects = webapp.persistency_provider.list_projects()
    projects = [p.replace(".mitzu", "") for p in projects]
    res = (
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem(value_to_label(p), href=p, external_link=True)
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
        curr_path_project_name = get_path_project_name(parse_result, webapp.app)

        if not curr_path_project_name:
            return "Select project"
        return value_to_label(curr_path_project_name)

    return res
