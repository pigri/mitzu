from __future__ import annotations

from typing import List, Tuple
from urllib.parse import urlparse

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.webapp.webapp as WA
from dash import Input, Output
from mitzu.helper import LOGGER
from mitzu.helper import value_to_label

CHOOSE_PROJECT_DROPDOWN = "choose-project-dropdown"


def create_dropdown_options(webapp: WA.MitzuWebApp):
    projects = webapp.persistency_provider.list_projects()
    projects = [p.replace(".mitzu", "") for p in projects]

    if len(projects) > 0:
        return [
            dbc.DropdownMenuItem(
                value_to_label(p), href=webapp.app.get_relative_path(f"/{p}")
            )
            for p in projects
        ]
    else:
        return [dbc.DropdownMenuItem("Could not find any projects", disabled=True)]


def create_project_dropdown(webapp: WA.MitzuWebApp):
    dropdown_items = create_dropdown_options(webapp)
    res = dbc.DropdownMenu(
        children=dropdown_items,
        id=CHOOSE_PROJECT_DROPDOWN,
        in_navbar=True,
        label="Select project",
        size="sm",
        color="primary",
    )

    @webapp.app.callback(
        Output(CHOOSE_PROJECT_DROPDOWN, "label"),
        Output(CHOOSE_PROJECT_DROPDOWN, "children"),
        Input(WA.MITZU_LOCATION, "href"),
    )
    def update(href: str) -> Tuple[str, List[bc.Component]]:
        dropdown_items = create_dropdown_options(webapp)
        parse_result = urlparse(href)
        LOGGER.debug(f"Parse Result Dropdown {parse_result} - {href}")
        curr_path_project_name = webapp.get_path_project_name(parse_result)

        LOGGER.debug(
            f"Curr project pathname: {curr_path_project_name} - {parse_result} - {href}"
        )
        if not curr_path_project_name:
            return "Select project", dropdown_items
        return (value_to_label(curr_path_project_name), dropdown_items)

    return res
