from __future__ import annotations

from typing import Dict, List, Optional, Union

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.all_segments as AS
import mitzu.webapp.complex_segment as CS
import mitzu.webapp.dates_selector as DS
import mitzu.webapp.metrics_config as MC
import mitzu.webapp.navbar.metric_type_dropdown as MNB
import mitzu.webapp.webapp as WA
from dash import dcc, html
from dash.dependencies import Input, Output, State
from mitzu.webapp.complex_segment import ComplexSegmentCard
from mitzu.webapp.helper import (
    deserialize_component,
    find_components,
    find_event_field_def,
    find_first_component,
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

        conv_window_interval = find_first_component(
            MC.CONVERSION_WINDOW_INTERVAL, mc_children
        ).value
        conv_window_interval_steps = find_first_component(
            MC.CONVERSION_WINDOW_INTERVAL_STEPS, mc_children
        ).value

        date_selector = find_first_component(DS.DATE_SELECTOR, mc_children)
        time_group = DS.get_metric_timegroup(date_selector)
        lookback_days = DS.get_metric_lookback_days(date_selector)
        start_date, end_date = None, None
        if lookback_days is None:
            start_date, end_date = DS.get_metric_custom_dates(date_selector)

        group_by_path = find_components(
            CS.COMPLEX_SEGMENT_GROUP_BY, all_seg_children[0]
        )[0].value
        group_by = None
        if group_by_path is not None:
            group_by = find_event_field_def(group_by_path, discovered_datasource)

        if len(segments) > 1 and isinstance(metric, M.Conversion):
            conv_window = M.TimeWindow(
                conv_window_interval, M.TimeGroup(conv_window_interval_steps)
            )
            return metric.config(
                time_group=M.TimeGroup(time_group),
                conv_window=conv_window,
                group_by=group_by,
                lookback_days=lookback_days,
                start_dt=start_date,
                end_dt=end_date,
                custom_title="",
            )
        elif isinstance(metric, M.Segment):
            return metric.config(
                time_group=M.TimeGroup(time_group),
                group_by=group_by,
                lookback_days=lookback_days,
                start_dt=start_date,
                end_dt=end_date,
                custom_title="",
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
