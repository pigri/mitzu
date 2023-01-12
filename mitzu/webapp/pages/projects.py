from dash import register_page
import dash_bootstrap_components as dbc
from dash import html, no_update
import mitzu.helper as H
import dash.development.base_component as bc
import flask
import mitzu.webapp.dependencies as DEPS
import mitzu.model as M
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
from dash import Input, Output, callback, dcc
from typing import List
import base64
import traceback


PROJECTS_CONTAINER = "projects-container"
ERROR_PLACE_HOLDER = "error-place-holder"
UPLOAD_BUTTON = "file_upload"

register_page(
    __name__,
    path=P.PROJECTS_PATH,
    title="Mitzu - Explore",
)


def layout() -> bc.Component:
    projects = create_projects_children()
    return html.Div(
        [
            NB.create_mitzu_navbar("explore-navbar", []),
            dbc.Container(
                children=[
                    html.H4("Your projects", className="card-title"),
                    html.Hr(),
                    html.Div(children=projects, id=PROJECTS_CONTAINER),
                    html.Hr(),
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Button(
                                    "Create new project",
                                    color="primary",
                                    href=P.PROJECTS_CREATE_PATH,
                                ),
                                sm=12,
                                lg=2,
                            ),
                            dbc.Col(
                                dcc.Upload(
                                    "Upload Mitzu File",
                                    id=UPLOAD_BUTTON,
                                    className="btn btn-secondary",
                                    multiple=False,
                                    accept=".mitzu",
                                ),
                                sm=12,
                                lg=2,
                            ),
                        ],
                        class_name="mb-3",
                    ),
                    html.Div(id=ERROR_PLACE_HOLDER, children=[]),
                ]
            ),
        ]
    )


def create_projects_children() -> List[bc.Component]:
    depenednecies: DEPS.Dependencies = flask.current_app.config.get(DEPS.CONFIG_KEY)
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

        return dbc.Row(children=projects)

    return html.H4(
        "You don't have any projects yet...", className="card-title text-center"
    )


def create_project_selector(project_id: str, deps: DEPS.Dependencies) -> bc.Component:
    discovered_project = deps.storage.get_discovered_project(project_id)
    project = discovered_project.project

    tables = len(project.event_data_tables)
    events = len(discovered_project.get_all_events())
    project_jumbotron = dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(
                        H.value_to_label(project.project_name), className="card-title"
                    ),
                    html.Hr(),
                    html.Img(
                        src=f"/assets/warehouse/{str(project.connection.connection_type.name).lower()}.png",
                        height=40,
                    ),
                    html.P(f"This project has {events} events in {tables} datasets."),
                    html.P(
                        "More description will come here for this project..."
                    ),  # TBD Support project desc
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


def store_discovered_project(
    dp: M.DiscoveredProject,
):
    deps: DEPS.Dependencies = flask.current_app.config.get(DEPS.CONFIG_KEY)

    deps.storage.set_project(dp.project.id, dp.project)
    deps.storage.set_connection(dp.project.connection.id, dp.project.connection)
    for edt, defs in dp.definitions.items():
        edt_full_name = edt.get_full_name()
        deps.storage.set_event_data_table_definition(dp.project.id, edt_full_name, defs)


@callback(
    Output(PROJECTS_CONTAINER, "children"),
    Output(ERROR_PLACE_HOLDER, "children"),
    Input(UPLOAD_BUTTON, "contents"),
    prevent_initial_call=True,
)
def update_output(content: str):
    if content is not None:
        try:
            _, content_string = content.split(",")
            decoded = base64.b64decode(content_string)
            discovered_project = M.DiscoveredProject.deserialize(decoded)
            store_discovered_project(discovered_project)
        except Exception as exc:
            traceback.print_exc()
            return no_update, html.Pre(
                f"Error :( \n{str(exc)}", className="text-danger"
            )
        return create_projects_children(), []
    else:
        return no_update, []
