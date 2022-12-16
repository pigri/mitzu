from __future__ import annotations


import dash_bootstrap_components as dbc
import flask
from mitzu.helper import value_to_label
import mitzu.webapp.dependencies as DEPS

CHOOSE_PROJECT_DROPDOWN = "choose-project-dropdown"


def create_dropdown_options():
    dependencies: DEPS.Dependencies = flask.current_app.config.get(DEPS.CONFIG_KEY)
    projects = dependencies.storage.list_projects()

    if len(projects) > 0:
        return [
            dbc.DropdownMenuItem(
                value_to_label(p),
                href=f"/explore/{p}",
            )
            for p in projects
        ]
    else:
        return [dbc.DropdownMenuItem("Could not find any projects", disabled=True)]


def create_project_dropdown(project_name: str):
    dropdown_items = create_dropdown_options()
    res = dbc.DropdownMenu(
        children=dropdown_items,
        id=CHOOSE_PROJECT_DROPDOWN,
        label=project_name,
        color="warning",
        className="mb-3",
        caret=True,
    )
    return res
