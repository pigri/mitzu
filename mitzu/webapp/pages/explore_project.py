from typing import cast

import dash.development.base_component as bc
import flask
from dash import html, register_page

import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.explore.explore_page as EXP
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted_layout
import traceback

register_page(
    __name__,
    path_template=P.PROJECTS_EXPLORE_PATH,
    title="Mitzu - Explore",
)


@restricted_layout
def layout(project_id: str, **query_params) -> bc.Component:
    try:
        depenednecies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        project = depenednecies.storage.get_project(project_id)

        if project is None:
            return html.Div("Project not found", className="d-flex text-center lead")

        discovered_project = project._discovered_project.get_value()
        if discovered_project is None:
            return html.Div(
                "Project have not been discovered yet",
                className="d-flex text-center lead",
            )

        return EXP.create_explore_page(
            query_params=query_params,
            discovered_project=discovered_project,
            storage=depenednecies.storage,
        )
    except Exception as exc:
        traceback.print_exc()
        return html.Div(str(exc))


EXP.create_callbacks()
