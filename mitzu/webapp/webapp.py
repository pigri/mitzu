from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.serialization as SE
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.complex_segment_handler as CS
import mitzu.webapp.dates_selector_handler as DS
import mitzu.webapp.event_segment_handler as ES
import mitzu.webapp.graph_handler as GH
import mitzu.webapp.metric_config_handler as MC
import mitzu.webapp.metric_segments_handler as MS
import mitzu.webapp.navbar.metric_type_handler as MNB
import mitzu.webapp.navbar.navbar as MN
import mitzu.webapp.simple_segment_handler as SS
import mitzu.webapp.toolbar_handler as TH
import mitzu.webapp.webapp as WA
from dash import Dash, ctx, dcc, html
from dash.dependencies import ALL, Input, Output, State
from mitzu.webapp.helper import (
    LOGGER,
    deserialize_component,
    find_components,
    find_event_field_def,
    get_path_project_name,
)
from mitzu.webapp.persistence import PersistencyProvider

MAIN = "main"
MITZU_LOCATION = "mitzu_location"
ALL_INPUT_COMPS = {
    "all_inputs": {
        "href": Input(WA.MITZU_LOCATION, "href"),
        "metric_type_value": Input(MNB.METRIC_TYPE_DROPDOWN, "value"),
        "event_name_dd_value": Input(
            {"type": ES.EVENT_NAME_DROPDOWN, "index": ALL}, "value"
        ),
        "property_operator_dd_value": Input(
            {"type": SS.PROPERTY_OPERATOR_DROPDOWN, "index": ALL}, "value"
        ),
        "property_name_dd_value": Input(
            {"type": SS.PROPERTY_NAME_DROPDOWN, "index": ALL}, "value"
        ),
        "property_value_input": Input(
            {"type": SS.PROPERTY_VALUE_INPUT, "index": ALL}, "value"
        ),
        "group_by_dd_value": Input(
            {"type": CS.COMPLEX_SEGMENT_GROUP_BY, "index": ALL}, "value"
        ),
        "time_group_dd_value": Input(DS.TIME_GROUP_DROPDOWN, "value"),
        "custom_start_date_value": Input(DS.CUSTOM_DATE_PICKER, "start_date"),
        "custom_end_date_value": Input(DS.CUSTOM_DATE_PICKER, "end_date"),
        "lookback_dd_value": Input(DS.LOOKBACK_WINDOW_DROPDOWN, "value"),
        "conv_window_tg_dd_value": Input(MC.CONVERSION_WINDOW_INTERVAL_STEPS, "value"),
        "conv_window_interval_value": Input(MC.CONVERSION_WINDOW_INTERVAL, "value"),
        "agg_type_dd_value": Input(MC.AGGREGATION_TYPE, "value"),
        "refresh_n_clicks": Input(TH.GRAPH_REFRESH_BUTTON, "n_clicks"),
        "chart_button_n_clicks": Input(TH.CHART_BUTTON, "n_clicks"),
        "table_button_n_clicks": Input(TH.TABLE_BUTTON, "n_clicks"),
        "sql_button_n_clicks": Input(TH.SQL_BUTTON, "n_clicks"),
    }
}


@dataclass
class MitzuWebApp:

    app: Dash
    persistency_provider: PersistencyProvider
    authorizer: Optional[AUTH.MitzuAuthorizer]
    discovered_project_cache: Dict[str, M.DiscoveredProject] = field(
        default_factory=lambda: {}
    )

    def init_app(self):
        LOGGER.info("Initializing WebApp")
        loc = dcc.Location(id=MITZU_LOCATION, refresh=False)
        navbar = MN.create_mitzu_navbar(self)

        metric_segments_div = MS.MetricSegmentsHandler.from_metric(
            discovered_project=None,
            metric=None,
            metric_type=MNB.MetricType.SEGMENTATION,
        ).component

        graph_container = self.create_graph_container()
        self.app.layout = html.Div(
            children=[
                loc,
                navbar,
                dbc.Container(
                    children=[
                        dbc.Row(
                            children=[
                                dbc.Col(metric_segments_div, lg=4, md=12),
                                dbc.Col(graph_container, lg=8, md=12),
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
        self.create_callbacks()

    def get_discovered_project(self, project_name) -> Optional[M.DiscoveredProject]:
        if not project_name:
            return None
        dp = self.discovered_project_cache.get(project_name)
        if dp is not None:
            return dp

        LOGGER.info(f"Loading project: {project_name}")
        dp = self.persistency_provider.get_project(project_name)
        if dp is not None:
            self.discovered_project_cache[project_name] = dp
        return dp

    def create_graph_container(self):
        metrics_config_card = MC.MetricConfigHandler.from_metric(None, None).component
        graph_handler = GH.GraphHandler.from_webapp(self)
        graph_handler.create_callbacks()
        graph = graph_handler.component
        toolbar_handler = TH.ToolbarHandler.from_webapp(self)
        toolbar_handler.create_callbacks()
        toolbar = toolbar_handler.component
        graph_container = dbc.Card(
            children=[
                dbc.CardBody(
                    children=[
                        metrics_config_card,
                        toolbar,
                        graph,
                    ],
                ),
            ],
        )
        return graph_container

    def get_metric_from_query(
        self, query: str, project_name
    ) -> Tuple[Optional[M.Metric], MNB.MetricType]:
        discovered_project = self.get_discovered_project(project_name)
        if discovered_project is None:
            return None, MNB.MetricType.SEGMENTATION
        try:
            metric = SE.from_compressed_string(query, discovered_project.project)
        except Exception:
            metric = None

        metric_type = MNB.MetricType.from_metric(metric)
        return metric, metric_type

    def create_metric_from_components(
        self,
        metric_seg_children: List[bc.Component],
        mc_children: List[bc.Component],
        discovered_project: Optional[M.DiscoveredProject],
        metric_type: MNB.MetricType,
    ) -> Optional[M.Metric]:
        if discovered_project is None:
            return None

        segments = MS.MetricSegmentsHandler.from_component(
            discovered_project, html.Div(children=metric_seg_children)
        ).to_metric_segments()
        metric: Optional[Union[M.Segment, M.Conversion]] = None
        if metric_type == MNB.MetricType.CONVERSION:
            metric = M.Conversion(segments)
        elif metric_type == MNB.MetricType.SEGMENTATION:
            if len(segments) >= 1:
                metric = segments[0]

        if metric is None:
            return None

        metric_config_comp = MC.MetricConfigHandler.from_component(
            html.Div(children=mc_children), discovered_project
        )
        metric_config, conv_tw = metric_config_comp.to_metric_config_and_conv_window()
        if metric_config.agg_type:
            agg_str = M.AggType.to_agg_str(
                metric_config.agg_type, metric_config.agg_param
            )
        else:
            agg_str = None

        group_by = None
        if len(metric_seg_children) > 0:
            group_by_paths = find_components(
                CS.COMPLEX_SEGMENT_GROUP_BY, metric_seg_children[0]
            )
            if len(group_by_paths) == 1:
                gp = group_by_paths[0].value
                group_by = find_event_field_def(gp, discovered_project) if gp else None

        if isinstance(metric, M.Conversion):
            return metric.config(
                time_group=metric_config.time_group,
                conv_window=conv_tw,
                group_by=group_by,
                lookback_days=metric_config.lookback_days,
                start_dt=metric_config.start_dt,
                end_dt=metric_config.end_dt,
                custom_title="",
                aggregation=agg_str,
            )
        elif isinstance(metric, M.Segment):
            return metric.config(
                time_group=metric_config.time_group,
                group_by=group_by,
                lookback_days=metric_config.lookback_days,
                start_dt=metric_config.start_dt,
                end_dt=metric_config.end_dt,
                custom_title="",
                aggregation=agg_str,
            )
        raise Exception("Invalid metric type")

    def handle_metric_changes(
        self,
        parse_result: ParseResult,
        discovered_project: M.DiscoveredProject,
        metric_seg_divs: List[Dict],
        metric_configs: List[Dict],
        metric_type_value: str,
    ) -> Tuple[Optional[M.Metric], MNB.MetricType]:
        metric: Optional[M.Metric] = None
        metric_type = MNB.MetricType.SEGMENTATION
        project_name = get_path_project_name(parse_result, self.app)
        if ctx.triggered_id == MITZU_LOCATION:
            query = parse_result.query[2:]
            metric, metric_type = self.get_metric_from_query(query, project_name)
        else:
            metric_seg_children = [deserialize_component(c) for c in metric_seg_divs]
            metric_configs_children = [deserialize_component(c) for c in metric_configs]
            metric_type = MNB.MetricType(metric_type_value)
            metric = self.create_metric_from_components(
                metric_seg_children,
                metric_configs_children,
                discovered_project,
                metric_type,
            )
        return metric, metric_type

    def create_callbacks(self):
        SS.SimpleSegmentHandler.create_callbacks(self.app)

        @self.app.callback(
            output=[
                Output(MS.METRIC_SEGMENTS, "children"),
                Output(MC.METRICS_CONFIG_CONTAINER, "children"),
                Output(MITZU_LOCATION, "search"),
                Output(MNB.METRIC_TYPE_DROPDOWN, "value"),
            ],
            inputs=ALL_INPUT_COMPS,
            state=dict(
                metric_segment_divs=State(MS.METRIC_SEGMENTS, "children"),
                metric_configs=State(MC.METRICS_CONFIG_CONTAINER, "children"),
            ),
            prevent_initial_call=True,
        )
        def change_layout(
            all_inputs: Dict[str, Any],
            metric_segment_divs: List[Dict],
            metric_configs: List[Dict],
        ) -> Tuple[List[html.Div], List[html.Div], str, str]:
            LOGGER.debug(f"Layout changed caused by: {ctx.triggered_id}")
            parse_result = urlparse(all_inputs["href"])
            project_name = get_path_project_name(parse_result, self.app)
            discovered_project = self.get_discovered_project(project_name)

            if discovered_project is None:
                def_mc_comp = MC.MetricConfigHandler.from_metric(None, None)
                def_mc_children = def_mc_comp.component.children
                return (
                    [],
                    [c.to_plotly_json() for c in def_mc_children],
                    "?" + parse_result.query[2:],
                    MNB.MetricType.SEGMENTATION.value,
                )

            metric, metric_type = self.handle_metric_changes(
                parse_result=parse_result,
                discovered_project=discovered_project,
                metric_seg_divs=metric_segment_divs,
                metric_configs=metric_configs,
                metric_type_value=all_inputs["metric_type_value"],
            )

            url_search = "?"
            if metric is not None:
                url_search = "?m=" + SE.to_compressed_string(metric)

            metric_segments = MS.MetricSegmentsHandler.from_metric(
                discovered_project=discovered_project,
                metric=metric,
                metric_type=metric_type,
            ).component.children

            metric_segment_comps = [seg.to_plotly_json() for seg in metric_segments]

            mc_children = MC.MetricConfigHandler.from_metric(
                metric, discovered_project
            ).component.children
            metric_config_comps = [c.to_plotly_json() for c in mc_children]

            return (
                metric_segment_comps,
                metric_config_comps,
                url_search,
                metric_type.value,
            )
