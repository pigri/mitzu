import sys

import dash_bootstrap_components as dbc
import mitzu.model as M
from dash import Dash, html
from mitzu.project import load_project_from_file
from mitzu.webapp.all_segments import AllSegmentsContainer
from mitzu.webapp.graph import GraphContainer
from mitzu.webapp.metrics_config import MetricsConfigDiv
from mitzu.webapp.navbar.navbar import MitzuNavbar

MAIN = "main"


def init_app(app: Dash, dataset_model: M.DatasetModel):

    all_segments = AllSegmentsContainer(dataset_model)
    metrics_config = MetricsConfigDiv()
    graph = GraphContainer()
    navbar = MitzuNavbar()

    app.layout = html.Div(
        children=[navbar, all_segments, metrics_config, graph],
        className=MAIN,
        id=MAIN,
    )
    requested_graph = M.ProtectedState[M.Metric]()
    current_graph = M.ProtectedState[M.Metric]()

    MitzuNavbar.create_callbacks(app)
    AllSegmentsContainer.create_callbacks(app, dataset_model)
    GraphContainer.create_callbacks(app, dataset_model, requested_graph, current_graph)


def __create_dash_debug_server(project_name: str):
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
    app._favicon = "favicon_io/favicon.ico"
    project = load_project_from_file(project_name)
    init_app(app, project)
    app.run_server(debug=True)


if __name__ == "__main__":
    __create_dash_debug_server(sys.argv[1])
