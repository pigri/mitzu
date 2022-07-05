from __future__ import annotations

from typing import Dict, List, Optional, Union

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.all_segments as AS
import mitzu.webapp.metrics_config as MC
import mitzu.webapp.navbar.metric_type_dropdown as MNB
import mitzu.webapp.webapp as WA
from dash import dcc, html
from dash.dependencies import Input, Output, State
from mitzu.webapp.complex_segment import ComplexSegmentCard
from mitzu.webapp.helper import (
    deserialize_component,
    find_component,
    find_event_field_def,
)

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
                            children=[html.B(className="bi bi-arrow-clockwise")],
                            size="sm",
                            color="primary",
                            className=GRAPH_REFRESH_BUTTON,
                            id=GRAPH_REFRESH_BUTTON,
                            style={"margin-right": "10px"},
                        ),
                        dbc.ButtonGroup(
                            [
                                dbc.Button(
                                    html.B(className="bi bi-bar-chart-line"),
                                    size="sm",
                                    outline=True,
                                    color="info",
                                ),
                                dbc.Button(
                                    html.B(className="bi bi-graph-up"),
                                    size="sm",
                                    outline=True,
                                    color="info",
                                ),
                                dbc.Button(
                                    html.B(className="bi bi-bezier2"),
                                    size="sm",
                                    outline=True,
                                    color="info",
                                    disabled=True,
                                ),
                                dbc.Button(
                                    html.B(className="bi bi-grid-3x3-gap-fill"),
                                    size="sm",
                                    outline=True,
                                    color="info",
                                    disabled=True,
                                ),
                            ],
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
    def create_metric(
        cls,
        all_seg_children: List[ComplexSegmentCard],
        mc_children: List[bc.Component],
        discovered_datasource: M.DiscoveredEventDataSource,
        metric_type: str,
    ) -> Optional[M.Metric]:
        segments = AS.AllSegmentsContainer.get_segments(
            all_seg_children, discovered_datasource, metric_type
        )
        metric: Optional[Union[M.Segment, M.Conversion]] = None
        for seg in segments:
            if metric is None:
                metric = seg
            else:
                metric = metric >> seg
        if metric is None:
            return None

        time_group_value = find_component(MC.TIME_GROUP_DROWDOWN, mc_children).value

        time_window_interval = find_component(
            MC.TIME_WINDOW_INTERVAL, mc_children
        ).value
        time_window_interval_steps = find_component(
            MC.TIME_WINDOW_INTERVAL_STEPS, mc_children
        ).value

        dates = find_component(MC.DATE_RANGE_INPUT, mc_children)

        group_by_path = (
            all_seg_children[0].children[2].children[0].children[1].children[0].value
        )
        group_by = None
        if group_by_path is not None:
            group_by = find_event_field_def(group_by_path, discovered_datasource)

        if len(segments) > 1 and isinstance(metric, M.Conversion):
            conv_window = M.TimeWindow(
                time_window_interval, M.TimeGroup(time_window_interval_steps)
            )
            return metric.config(
                time_group=M.TimeGroup(time_group_value),
                conv_window=conv_window,
                group_by=group_by,
                start_dt=dates.start_date,
                end_dt=dates.end_date,
            )
        elif isinstance(metric, M.Segment):
            return metric.config(
                time_group=M.TimeGroup(time_group_value),
                group_by=group_by,
                start_dt=dates.start_date,
                end_dt=dates.end_date,
            )
        raise Exception("Invalid metric type")

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

            metric = cls.create_metric(
                all_seg_children, metric_configs_children, dm, metric_type
            )
            res = cls.create_graph(metric)
            return [res]
