from typing import Dict, List, Optional, Union

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
from dash import Dash, dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from mitzu.webapp.all_segments import ALL_SEGMENTS, AllSegmentsContainer
from mitzu.webapp.complex_segment import ComplexSegment
from mitzu.webapp.helper import deserialize_component, find_property_class
from mitzu.webapp.metrics_config import METRICS_CONFIG

GRAPH = "graph"
GRAPH_CONTAINER = "graph_container"
GRAPH_CONTAINER_HEADER = "graph_container_header"
GRAPH_CONTAINER_AUTOFREFRESH = "graph_auto_refresh"
GRAPH_REFRESH_BUTTON = "graph_refresh_button"
GRAPH_REFRESH_INTERVAL = "graph_refresh_interval"


class GraphContainer(dbc.Card):
    def __init__(self):
        super().__init__(
            children=[
                dbc.CardHeader(
                    id=GRAPH_CONTAINER_HEADER,
                    className=GRAPH_CONTAINER_HEADER,
                    children=[
                        dbc.Button(
                            children=[
                                html.I(className="bi bi-arrow-clockwise"),
                                "Refresh",
                            ],
                            size="sm",
                            className=GRAPH_REFRESH_BUTTON,
                            id=GRAPH_REFRESH_BUTTON,
                        ),
                        # dbc.Switch(
                        #     label="Auto refresh",
                        #     value=False,
                        #     className=GRAPH_CONTAINER_AUTOFREFRESH,
                        #     id=GRAPH_CONTAINER_AUTOFREFRESH,
                        # ),
                    ],
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
                dcc.Interval(
                    id=GRAPH_REFRESH_INTERVAL,
                    interval=750,
                    disabled=True,
                    n_intervals=0,
                ),
            ],
        )

    @classmethod
    def create_metric(
        cls,
        all_seg_children: List[ComplexSegment],
        metric_configs_children: List[bc.Component],
        dataset_model: M.DatasetModel,
    ) -> Optional[M.Metric]:
        segments = AllSegmentsContainer.get_segments(all_seg_children, dataset_model)
        metric: Optional[Union[M.Segment, M.Conversion]] = None
        for seg in segments:
            if metric is None:
                metric = seg
            else:
                metric = metric >> seg
        if metric is None:
            return None

        time_group_value = metric_configs_children[1].children[1].children[1].value
        time_window_interval = (
            metric_configs_children[1].children[2].children[1].children[0].value
        )
        time_window_interval_steps = (
            metric_configs_children[1].children[2].children[1].children[1].value
        )
        dates = metric_configs_children[1].children[0].children[1].children[0]

        group_by_path = all_seg_children[0].children[2].children[0].value
        group_by = None
        if group_by_path is not None:
            group_by = find_property_class(group_by_path, dataset_model)

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
        return dcc.Graph(
            id=GRAPH,
            figure=metric.get_figure() if metric is not None else {},
            config={"displayModeBar": False},
        )

    @classmethod
    def create_callbacks(
        cls,
        app: Dash,
        dataset_model: M.DatasetModel,
        requested_graph: M.ProtectedState[M.Metric],
        current_graph: M.ProtectedState[M.Metric],
    ):
        @app.callback(
            Output(GRAPH_REFRESH_INTERVAL, "disabled"),
            Input(GRAPH_CONTAINER_AUTOFREFRESH, "value"),
            prevent_initial_call=True,
        )
        def autorefresh_callback(value: bool) -> bool:
            print(f"Auto refresh {value}")
            return not value

        @app.callback(
            Output(GRAPH_CONTAINER, "children"),
            [
                Input(GRAPH_REFRESH_BUTTON, "n_clicks"),
                Input(GRAPH_REFRESH_INTERVAL, "n_intervals"),
            ],
            [
                State(ALL_SEGMENTS, "children"),
                State(METRICS_CONFIG, "children"),
            ],
            prevent_initial_call=True,
        )
        def input_changed(
            n_intervals: int,
            n_clicks: int,
            all_segments: List[Dict],
            metric_configs: List[Dict],
        ) -> List[List]:
            if requested_graph.has_value():
                raise PreventUpdate()

            all_seg_children: List[bc.Component] = [
                deserialize_component(child) for child in all_segments
            ]
            metric_configs_children: List[bc.Component] = [
                deserialize_component(child) for child in metric_configs
            ]
            metric = cls.create_metric(
                all_seg_children, metric_configs_children, dataset_model
            )

            # curr_metric = current_graph.get_value()
            # if metric == curr_metric:
            #     if (
            #         metric is not None
            #         and curr_metric is not None
            #         and curr_metric._config == metric._config
            #     ):
            #         print("prevent update")
            #         raise PreventUpdate()

            requested_graph.set_value(metric)
            res = cls.create_graph(metric)
            current_graph.set_value(metric)
            requested_graph.set_value(None)
            return [res]
