from __future__ import annotations

import sys
from dataclasses import dataclass

import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.navbar.navbar as MN
from dash import Dash, dcc, html
from mitzu.project import load_project_from_file
from mitzu.webapp.all_segments import AllSegmentsContainer
from mitzu.webapp.graph import GraphContainer
from mitzu.webapp.metrics_config import MetricsConfigDiv
from mitzu.webapp.persistence import PathPersistencyProvider, PersistencyProvider

MAIN = "main"
PATH_PROJECTS = "projects"
PATH_RESULTS = "results"
MITZU_LOCATION = "mitzu_location"
MAIN_CONTAINER = "main_container"


@dataclass(frozen=True)
class MitzuWebApp:

    persistency_provider: PersistencyProvider
    app: Dash

    def init_app(self):
        loc = dcc.Location(id=MITZU_LOCATION)
        navbar = MN.MitzuNavbar(self)
        all_segments = AllSegmentsContainer(dataset_model)
        metrics_config = MetricsConfigDiv()
        graph = GraphContainer()

        self.app.layout = html.Div(
            children=[navbar, loc, all_segments, metrics_config, graph],
            className=MAIN,
            id=MAIN,
        )

        requested_graph = M.ProtectedState[M.Metric]()
        current_graph = M.ProtectedState[M.Metric]()

        MN.MitzuNavbar.create_callbacks(self.app)
        AllSegmentsContainer.create_callbacks(self.app, dataset_model)
        GraphContainer.create_callbacks(
            self.app, dataset_model, requested_graph, current_graph
        )


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
