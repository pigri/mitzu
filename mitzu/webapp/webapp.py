from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.all_segments as AS
import mitzu.webapp.metrics_config as MC
import mitzu.webapp.navbar.metric_type_dropdown as MNB
import mitzu.webapp.navbar.navbar as MN
from dash import Dash, dcc, html
from mitzu.webapp.graph import GraphContainer
from mitzu.webapp.persistence import PersistencyProvider

MAIN = "main"
PATH_PROJECTS = "projects"
PATH_RESULTS = "results"
MITZU_LOCATION = "mitzu_location"
MAIN_CONTAINER = "main_container"
PROJECT_PATH_INDEX = 1
METRIC_TYPE_PATH_INDEX = 2


@dataclass
class MitzuWebApp:

    persistency_provider: PersistencyProvider
    app: Dash

    _discovered_datasource: M.ProtectedState[
        M.DiscoveredEventDataSource
    ] = M.ProtectedState[M.DiscoveredEventDataSource]()
    _current_project: Optional[str] = None

    def get_discovered_datasource(self) -> Optional[M.DiscoveredEventDataSource]:
        return self._discovered_datasource.get_value()

    def load_dataset_model(self, pathname: str):
        path_parts = pathname.split("/")
        curr_path_project_name = path_parts[PROJECT_PATH_INDEX]
        if (
            curr_path_project_name == self._current_project
            and self._discovered_datasource.has_value()
        ):
            return
        self._current_project = curr_path_project_name
        if curr_path_project_name:
            print(f"Loading project: {curr_path_project_name}")
            dd: Optional[
                M.DiscoveredEventDataSource
            ] = self.persistency_provider.get_item(
                f"{PATH_PROJECTS}/{curr_path_project_name}.mitzu"
            )
            if dd is not None:
                dd.source._discovered_event_datasource.set_value(dd)
            self._discovered_datasource.set_value(dd)

    def init_app(self):
        loc = dcc.Location(id=MITZU_LOCATION)
        navbar = MN.create_mitzu_navbar(self)

        all_segments = AS.AllSegmentsContainer(
            self._discovered_datasource.get_value(), MNB.SEGMENTATION
        )
        metrics_config = MC.MetricsConfigCard()
        graph = GraphContainer()

        self.app.layout = html.Div(
            children=[
                loc,
                navbar,
                dbc.Container(
                    children=[
                        dbc.Row(
                            children=[dbc.Col(html.Div(metrics_config))],
                            className="g-1 mb-1",
                        ),
                        dbc.Row(
                            children=[
                                dbc.Col(all_segments, lg=4, md=12, xl=3),
                                dbc.Col(graph, lg=8, md=12, xl=9),
                            ],
                            justify="start",
                            align="top",
                            className="g-1",
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
        MC.create_callbacks(self)
