from dash import register_page
import dash_bootstrap_components as dbc
from dash import html
import dash.development.base_component as bc
import flask
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.navbar as NB
from mitzu.webapp.auth.decorator import restricted_layout
import mitzu.webapp.pages.paths as P
from typing import List, cast
import traceback


PROJECTS_CONTAINER = "projects_container"
PROJECTS_ROW = "projects_row"
PROJECT_CARD_TITLE = "project_card_title"

register_page(
    __name__,
    path=P.PROJECTS_PATH,
    title="Mitzu - Explore",
)


@restricted_layout
def layout(**query_params) -> bc.Component:
    projects = create_projects_children()

    return html.Div(
        [
            NB.create_mitzu_navbar("explore-navbar", []),
            dbc.Container(
                children=[
                    dbc.Row(
                        [
                            dbc.Col(
                                html.H4(
                                    "Select a project for exploration",
                                    className="card-title",
                                ),
                                width="auto",
                            ),
                            dbc.Col(
                                dbc.Button(
                                    children=[
                                        html.I(className="bi bi-plus-circle me-1"),
                                        "Add project",
                                    ],
                                    href=P.PROJECTS_CREATE_PATH,
                                ),
                                width="auto",
                                class_name="ms-auto",
                            ),
                        ]
                    ),
                    html.Hr(),
                    html.Div(children=projects, id=PROJECTS_CONTAINER),
                    html.Hr(),
                ]
            ),
        ]
    )


def create_projects_children() -> List[bc.Component]:
    depenednecies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    project_ids = depenednecies.storage.list_projects()

    projects = []
    if len(project_ids) > 0:
        for p in project_ids:
            try:
                projects.append(create_project_selector(p, depenednecies))
            except Exception as exc:
                traceback.print_exc()
                projects.append(
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(html.P(str(exc), className="text-danger")),
                            class_name="mb-3",
                        ),
                        lg=3,
                        sm=12,
                    )
                )

        return dbc.Row(children=projects, id=PROJECTS_ROW)

    return html.H4(
        "You don't have any projects yet...", className="card-title text-center"
    )


def create_project_selector(project_id: str, deps: DEPS.Dependencies) -> bc.Component:
    project = deps.storage.get_project(project_id)
    discovered_project = project._discovered_project.get_value()

    tables = len(project.event_data_tables)
    events = len(discovered_project.get_all_events()) if discovered_project else 0
    project_jumbotron = dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(
                        project.project_name,
                        className="card-title",
                        id=PROJECT_CARD_TITLE,
                    ),
                    html.Hr(),
                    html.Img(
                        src=f"/assets/warehouse/{str(project.connection.connection_type.name).lower()}.png",
                        height=40,
                    ),
                    html.P(f"This project has {events} events in {tables} datasets."),
                    html.P(project.description),
                    html.Div(
                        [
                            dbc.Button(
                                "Explore",
                                color="primary",
                                class_name="me-3",
                                href=P.create_path(
                                    P.PROJECTS_EXPLORE_PATH, project_id=project_id
                                ),
                            ),
                            dbc.Button(
                                "Manage",
                                color="secondary",
                                href=P.create_path(
                                    P.PROJECTS_MANAGE_PATH, project_id=project_id
                                ),
                            ),
                        ],
                    ),
                ]
            ),
            class_name="mb-3",
        ),
        lg=3,
        sm=12,
    )
    return project_jumbotron
