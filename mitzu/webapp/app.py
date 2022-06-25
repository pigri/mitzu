import sys

import dash_bootstrap_components as dbc
import mitzu.model as M
from dash import Dash, html
from mitzu.project import load_project
from mitzu.webapp.all_segments import AllSegmentsContainer
from mitzu.webapp.graph import GraphContainer
from mitzu.webapp.metrics_config import MetricsConfigDiv

MAIN = "main"


def init_app(app: Dash, dataset_model: M.DatasetModel):

    all_segments = AllSegmentsContainer(dataset_model)
    metrics_config = MetricsConfigDiv()
    graph = GraphContainer()

    app.layout = html.Div(
        children=[all_segments, metrics_config, graph],
        className=MAIN,
        id=MAIN,
    )
    requested_graph = M.ProtectedState[M.Metric]()
    current_graph = M.ProtectedState[M.Metric]()

    AllSegmentsContainer.create_callbacks(app, dataset_model)
    GraphContainer.create_callbacks(app, dataset_model, requested_graph, current_graph)


def create_dash_debug_server(project_name: str):
    app = Dash(
        __name__,
        external_stylesheets=[
            dbc.themes.MATERIA,
            dbc.icons.BOOTSTRAP,
            "assets/layout.css",
            "assets/components.css",
        ],
    )
    app.config.suppress_callback_exceptions = True
    project = load_project(project_name)
    init_app(app, project)
    app.run_server(debug=True)


if __name__ == "__main__":
    create_dash_debug_server(sys.argv[1])
