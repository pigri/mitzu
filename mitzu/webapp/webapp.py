from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Optional

import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.navbar.navbar as MN
from dash import Dash, dcc, html
from mitzu.webapp.all_segments import AllSegmentsContainer
from mitzu.webapp.graph import GraphContainer
from mitzu.webapp.metrics_config import MetricsConfigDiv
from mitzu.webapp.persistence import PathPersistencyProvider, PersistencyProvider

MAIN = "main"
PATH_PROJECTS = "projects"
PATH_RESULTS = "results"
MITZU_LOCATION = "mitzu_location"
MAIN_CONTAINER = "main_container"
PROJECT_PATH_INDEX = 1


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

        print(f"Setting model {curr_path_project_name}")

        dd: M.DiscoveredEventDataSource = self.persistency_provider.get_item(
            f"{PATH_PROJECTS}/{curr_path_project_name}.mitzu"
        )
        self.dataset_model.set_value(dd.create_notebook_class_model())

    def init_app(self):
        loc = dcc.Location(id=MITZU_LOCATION)
        navbar = MN.create_mitzu_navbar(self)

        all_segments = AllSegmentsContainer(self.dataset_model.get_value())
        metrics_config = MetricsConfigDiv()
        graph = GraphContainer()

        self.app.layout = html.Div(
            children=[navbar, loc, all_segments, metrics_config, graph],
            className=MAIN,
            id=MAIN,
        )

        AllSegmentsContainer.create_callbacks(self)
        GraphContainer.create_callbacks(self)


def __create_dash_debug_server(base_path: str):
    app = Dash(
        __name__,
        external_stylesheets=[
            dbc.themes.MATERIA,
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
