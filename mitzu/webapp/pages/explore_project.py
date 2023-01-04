from dash import register_page, html
import mitzu.webapp.pages.explore.explore_page as EXP
import dash.development.base_component as bc
import flask
import mitzu.webapp.dependencies as DEPS
from typing import cast
import mitzu.webapp.pages.paths as P


register_page(
    __name__,
    path_template=P.PROJECTS_EXPLORE_PATH,
    title="Mitzu - Explore",
)


def layout(project_id: str, **query_params) -> bc.Component:
    depenednecies: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    discovered_project = depenednecies.storage.get_discovered_project(project_id)
    if discovered_project is None:
        return html.Div("Project not found", className="d-flex text-center lead")

    return EXP.create_explore_page(query_params, discovered_project)


EXP.create_callbacks()
