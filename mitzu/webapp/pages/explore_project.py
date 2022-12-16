from dash import register_page, html
import mitzu.webapp.pages.explore.explore_page as EXP
import dash.development.base_component as bc
import mitzu.helper as H
import flask
import mitzu.webapp.dependencies as DEPS
from typing import cast


def get_title(project_name: str):
    return f"Mitzu - {H.value_to_label(project_name)}"


register_page(
    __name__,
    path_template="/explore/<project_name>",
    title=get_title,
)


def layout(project_name: str, **query_params) -> bc.Component:
    depenednecies: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    discovered_project = depenednecies.storage.get_project(project_name)
    if discovered_project is None:
        return html.Div("Project not found", className="d-flex text-center lead")

    return EXP.create_explore_page(query_params, discovered_project)


EXP.create_callbacks()
