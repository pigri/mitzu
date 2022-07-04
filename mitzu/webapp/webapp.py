from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Optional

import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.all_segments as AS
import mitzu.webapp.navbar.navbar as MN
from dash import Dash, dcc, html
from mitzu.webapp.graph import GraphContainer
from mitzu.webapp.metrics_config import MetricsConfigDiv
from mitzu.webapp.persistence import PathPersistencyProvider, PersistencyProvider

MAIN = "main"
PATH_PROJECTS = "projects"
PATH_RESULTS = "results"
MITZU_LOCATION = "mitzu_location"
MAIN_CONTAINER = "main_container"
PROJECT_PATH_INDEX = 1
METRIC_TYPE_PATH_INDEX = 2

SEGMENTATION = "segmentation"
CONVERSION = "conversion"
RETENTION = "retention"
TIME_TO_CONVERT = "time_to_convert"
JOURNEY = "journey"


@dataclass
class MitzuWebApp:

    persistency_provider: PersistencyProvider
    app: Dash

    dataset_model: M.ProtectedState[M.DatasetModel] = M.ProtectedState[M.DatasetModel]()
    current_project: Optional[str] = None
    in_update: bool = False

    def set_dataset_model(self, pathname: str):
        path_parts = pathname.split("/")
        curr_path_project_name = path_parts[PROJECT_PATH_INDEX]
        if (
            curr_path_project_name == self.current_project
            and self.dataset_model.has_value()
        ):
            return
        self.current_project = curr_path_project_name
        if curr_path_project_name:
            dd: M.DiscoveredEventDataSource = self.persistency_provider.get_item(
                f"{PATH_PROJECTS}/{curr_path_project_name}.mitzu"
            )
            dd.source._discovered_event_datasource.set_value(dd)
            self.dataset_model.set_value(dd.create_notebook_class_model())

    def init_app(self):
        loc = dcc.Location(id=MITZU_LOCATION)
        navbar = MN.create_mitzu_navbar(self)

        all_segments = AS.AllSegmentsContainer(
            self.dataset_model.get_value(), SEGMENTATION
        )
        metrics_config = MetricsConfigDiv()
        graph = GraphContainer()

        self.app.layout = html.Div(
            children=[
                loc,
                navbar,
                dbc.Container(
                    children=[
                        dbc.Row(children=[dbc.Col(html.Div(metrics_config))]),
                        dbc.Row(
                            children=[
                                dbc.Col(all_segments, width=3),
                                dbc.Col(graph, width=9),
                            ],
                            class_name="flex-wrap",
                        ),
                    ],
                    fluid=True,
                ),
            ],
            className=MAIN,
            id=MAIN,
        )

        AS.AllSegmentsContainer.create_callbacks(self)
        GraphContainer.create_callbacks(self)


def __create_dash_debug_server(base_path: str):
    app = Dash(
        __name__,
        external_stylesheets=[
            dbc.themes.MINTY,
            dbc.icons.BOOTSTRAP,
            "assets/layout.css",
            "assets/components.css",
        ],
        title="Mitzu",
        suppress_callback_exceptions=True,
        assets_folder="assets",
    )
    web_app = MitzuWebApp(
        app=app, persistency_provider=PathPersistencyProvider(base_path)
    )
    web_app.init_app()
    app._favicon = "favicon_io/favicon.ico"
    app.run_server(debug=True)


if __name__ == "__main__":
    base_path = "tests/webapp"
    if len(sys.argv) == 2:
        base_path = sys.argv[1]
    __create_dash_debug_server(base_path)
