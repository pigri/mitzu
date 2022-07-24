from __future__ import annotations

from typing import Dict, List, Optional

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.all_segments as AS
import mitzu.webapp.metrics_config as MC
import mitzu.webapp.navbar.metric_type_dropdown as MNB
import mitzu.webapp.webapp as WA
from dash import dcc, html
from dash.dependencies import Input, Output, State
from mitzu.webapp.helper import deserialize_component
from mitzu.webapp.metric_builder import create_metric

GRAPH = "graph"
GRAPH_CONTAINER = "graph_container"
GRAPH_CONTAINER_HEADER = "graph_container_header"
GRAPH_CONTAINER_AUTOFREFRESH = "graph_auto_refresh"
GRAPH_REFRESH_BUTTON = "graph_refresh_button"


class GraphContainer(dbc.Card):
    def __init__(self):
        super().__init__(
            children=[
                dbc.CardHeader(
                    children=[
                        dbc.Button(
                            children=[html.B(className="bi bi-play-fill")],
                            size="sm",
                            color="info",
                            className=GRAPH_REFRESH_BUTTON,
                            id=GRAPH_REFRESH_BUTTON,
                            style={"margin-right": "10px"},
                        ),
                    ],
                    id=GRAPH_CONTAINER_HEADER,
                ),
                dbc.CardBody(
                    children=[
                        dcc.Loading(
                            className=GRAPH_CONTAINER,
                            id=GRAPH_CONTAINER,
                            type="dot",
                            children=[
                                dcc.Graph(
                                    id=GRAPH,
                                    className=GRAPH,
                                    figure={
                                        "data": [],
                                    },
                                    config={"displayModeBar": False},
                                )
                            ],
                        )
                    ],
                ),
            ],
        )

    @classmethod
    def create_graph(cls, metric: Optional[M.Metric]) -> dcc.Graph:
        fig = metric.get_figure() if metric is not None else {}

        return dcc.Graph(
            id=GRAPH,
            figure=fig,
            config={"displayModeBar": False},
        )

    @classmethod
    def create_callbacks(cls, webapp: WA.MitzuWebApp):
        @webapp.app.callback(
            Output(GRAPH_CONTAINER, "children"),
            [
                Input(GRAPH_REFRESH_BUTTON, "n_clicks"),
                Input(WA.MITZU_LOCATION, "pathname"),
            ],
            [
                State(MNB.METRIC_TYPE_DROPDOWN, "value"),
                State(AS.ALL_SEGMENTS, "children"),
                State(MC.METRICS_CONFIG, "children"),
            ],
            prevent_initial_call=True,
        )
        def input_changed(
            n_clicks: int,
            pathname: str,
            metric_type: str,
            all_segments: List[Dict],
            metric_configs: List[Dict],
        ) -> List[List]:
            webapp.load_dataset_model(pathname)
            all_seg_children: List[bc.Component] = [
                deserialize_component(child) for child in all_segments
            ]
            metric_configs_children: List[bc.Component] = [
                deserialize_component(child) for child in metric_configs
            ]
            dm = webapp.get_discovered_datasource()
            if dm is None:
                return []

            metric = create_metric(
                all_seg_children, metric_configs_children, dm, metric_type
            )
            res = cls.create_graph(metric)
            return [res]
