from dash import register_page
import dash_bootstrap_components as dbc
from dash import html
import mitzu.helper as H
import dash.development.base_component as bc
import flask
import mitzu.webapp.dependencies as DEPS
import mitzu.model as M
import mitzu.webapp.navbar as NB

EXPLORE_PATH = "/explore"
MANAGE_PROJECTS_BUTTON = "manage-projects-button"

register_page(
    __name__,
    path=EXPLORE_PATH,
    title="Mitzu - Explore",
)


def layout() -> bc.Component:
    depenednecies: DEPS.Dependencies = flask.current_app.config.get(DEPS.CONFIG_KEY)
    project_names = depenednecies.storage.list_projects()

    return html.Div(
        [
            NB.create_mitzu_navbar("explore-navbar", []),
            dbc.Container(
                children=[
                    html.H4("Choose from projects:", className="card-title"),
                    html.Hr(),
                    dbc.Row(
                        [create_project_selector(p) for p in project_names],
                        justify="center",
                    ),
                    html.Hr(),
                    dbc.Row(
                        [
                            dbc.Button(
                                [html.B(className="bi bi-gear"), " Manage projects"],
                                color="info",
                                class_name="mb-3 w-25",
                                href="/projects/",
                                id=MANAGE_PROJECTS_BUTTON,
                            ),
                        ],
                        justify="center",
                    ),
                ]
            ),
        ]
    )


def create_project_selector(project_name: str):
    deps: DEPS.Dependencies = flask.current_app.config.get(DEPS.CONFIG_KEY)
    discovered_project: M.DiscoveredProject = deps.storage.get_project(project_name)
    project = discovered_project.project

    tables = len(project.event_data_tables)
    events = len(discovered_project.get_all_events())
    project_jumbotron = dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(H.value_to_label(project_name), className="card-title"),
                    html.Hr(),
                    html.Img(
                        src=f"/assets/warehouse/{str(project.connection.connection_type.value).lower()}.png",
                        height=40,
                    ),
                    html.P(f"This project has {events} events in {tables} datasets."),
                    html.P(
                        "More description will come here for this project..."
                    ),  # TBD Support project desc
                    dbc.Button(
                        "Explore",
                        color="secondary",
                        outline=True,
                        href=f"{EXPLORE_PATH}/{project_name}",
                    ),
                ]
            ),
            class_name="mb-3",
        ),
        lg={"width": 4},
        sm={"width": 8},
    )

    return project_jumbotron
