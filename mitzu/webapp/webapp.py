from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import ParseResult, parse_qs, quote, unquote, urlparse

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
from dash import Dash, ctx, dcc, html
from dash.dependencies import ALL, Input, Output
from mitzu.helper import LOGGER
from mitzu.webapp.helper import (
    CHILDREN,
    METRIC_SEGMENTS,
    find_event_field_def,
    get_final_all_inputs,
)
from mitzu.webapp.persistence import PersistencyProvider

MITZU_LOCATION = "mitzu_location"
PROJECT_PATH_INDEX = 0

MAIN = "main"
ALL_INPUT_COMPS = {
    "all_inputs": {
        MITZU_LOCATION: Input(MITZU_LOCATION, "href"),
        MNB.METRIC_TYPE_DROPDOWN: Input(MNB.METRIC_TYPE_DROPDOWN, "value"),
        ES.EVENT_NAME_DROPDOWN: Input(
            {"type": ES.EVENT_NAME_DROPDOWN, "index": ALL}, "value"
        ),
        SS.PROPERTY_OPERATOR_DROPDOWN: Input(
            {"type": SS.PROPERTY_OPERATOR_DROPDOWN, "index": ALL}, "value"
        ),
        SS.PROPERTY_NAME_DROPDOWN: Input(
            {"type": SS.PROPERTY_NAME_DROPDOWN, "index": ALL}, "value"
        ),
        SS.PROPERTY_VALUE_INPUT: Input(
            {"type": SS.PROPERTY_VALUE_INPUT, "index": ALL}, "value"
        ),
        CS.COMPLEX_SEGMENT_GROUP_BY: Input(
            {"type": CS.COMPLEX_SEGMENT_GROUP_BY, "index": ALL}, "value"
        ),
        DS.TIME_GROUP_DROPDOWN: Input(DS.TIME_GROUP_DROPDOWN, "value"),
        DS.CUSTOM_DATE_PICKER_START_DATE: Input(DS.CUSTOM_DATE_PICKER, "start_date"),
        DS.CUSTOM_DATE_PICKER_END_DATE: Input(DS.CUSTOM_DATE_PICKER, "end_date"),
        DS.LOOKBACK_WINDOW_DROPDOWN: Input(DS.LOOKBACK_WINDOW_DROPDOWN, "value"),
        MC.TIME_WINDOW_INTERVAL_STEPS: Input(MC.TIME_WINDOW_INTERVAL_STEPS, "value"),
        MC.TIME_WINDOW_INTERVAL: Input(MC.TIME_WINDOW_INTERVAL, "value"),
        MC.AGGREGATION_TYPE: Input(MC.AGGREGATION_TYPE, "value"),
        TH.GRAPH_REFRESH_BUTTON: Input(TH.GRAPH_REFRESH_BUTTON, "n_clicks"),
        TH.CHART_BUTTON: Input(TH.CHART_BUTTON, "n_clicks"),
        TH.TABLE_BUTTON: Input(TH.TABLE_BUTTON, "n_clicks"),
        TH.SQL_BUTTON: Input(TH.SQL_BUTTON, "n_clicks"),
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
    fixed_project_name: Optional[str] = None
    results: Optional[Dict[str, Any]] = None

    def init_app(self):
        LOGGER.info("Initializing WebApp")
        loc = dcc.Location(id=MITZU_LOCATION, refresh=False)
        navbar = MN.create_mitzu_navbar(self)

        metric_segments_div = MS.from_metric(
            discovered_project=None,
            metric=None,
            metric_type=MNB.MetricType.SEGMENTATION,
        )

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

    def get_path_project_name(self, url_parse_result: ParseResult) -> Optional[str]:
        if self.fixed_project_name is not None:
            return self.fixed_project_name

        fixed_path = url_parse_result.path
        if not fixed_path.startswith("/"):
            fixed_path = f"/{fixed_path}"
        fixed_path = self.app.strip_relative_path(fixed_path)
        path_parts = fixed_path.split("/")
        return path_parts[PROJECT_PATH_INDEX]

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
        metrics_config_card = MC.from_metric(None, None)
        graph_handler = GH.create_graph_container()
        toolbar_handler = TH.create_toolbar_handler()

        toolbar = toolbar_handler
        graph_container = dbc.Card(
            children=[
                dbc.CardBody(
                    children=[
                        metrics_config_card,
                        toolbar,
                        graph_handler,
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
        except Exception as exc:
            LOGGER.exception(f"{query}:\n, {exc}")
            metric = None

        metric_type = MNB.MetricType.from_metric(metric)
        return metric, metric_type

    def create_metric_from_components(
        self,
        discovered_project: Optional[M.DiscoveredProject],
        metric_type: MNB.MetricType,
        all_inputs: Dict[str, Any],
    ) -> Optional[M.Metric]:
        if discovered_project is None:
            return None

        segments = MS.from_all_inputs(discovered_project, all_inputs)

        metric: Optional[Union[M.Segment, M.Conversion, M.RetentionMetric]] = None
        if metric_type == MNB.MetricType.CONVERSION:
            metric = M.Conversion(segments)
        elif metric_type == MNB.MetricType.SEGMENTATION:
            if len(segments) == 1:
                metric = segments[0]
        elif metric_type == MNB.MetricType.RETENTION:
            if len(segments) == 2:
                metric = segments[0] >= segments[1]
            elif len(segments) == 1:
                metric = segments[0] >= segments[0]

        if metric is None:
            return None

        metric_config, res_tw = MC.from_all_inputs(
            discovered_project, all_inputs, metric_type
        )
        if metric_config.agg_type:
            agg_str = M.AggType.to_agg_str(
                metric_config.agg_type, metric_config.agg_param
            )
        else:
            agg_str = None

        group_by = None
        group_by_paths = all_inputs[METRIC_SEGMENTS][CHILDREN]
        if len(group_by_paths) >= 1:
            gp = group_by_paths[0].get(CS.COMPLEX_SEGMENT_GROUP_BY)
            group_by = find_event_field_def(gp, discovered_project) if gp else None

        if isinstance(metric, M.Conversion):
            return metric.config(
                time_group=metric_config.time_group,
                conv_window=res_tw,
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
        elif isinstance(metric, M.RetentionMetric):
            return metric.config(
                time_group=metric_config.time_group,
                group_by=group_by,
                lookback_days=metric_config.lookback_days,
                start_dt=metric_config.start_dt,
                end_dt=metric_config.end_dt,
                retention_window=res_tw,
                custom_title="",
                aggregation=agg_str,
            )

        raise Exception("Invalid metric type")

    def metric_from_all_inputs(
        self,
        parse_result: ParseResult,
        discovered_project: M.DiscoveredProject,
        all_inputs: Dict[str, Any],
        ctx_triggered_id: str,
    ) -> Tuple[Optional[M.Metric], MNB.MetricType]:
        metric: Optional[M.Metric] = None
        metric_type = MNB.MetricType.SEGMENTATION
        project_name = self.get_path_project_name(parse_result)
        if ctx_triggered_id == MITZU_LOCATION:
            query = parse_qs(parse_result.query).get("m")
            if query is None:
                return None, MNB.MetricType.SEGMENTATION

            metric, metric_type = self.get_metric_from_query(
                unquote(query[0]), project_name
            )
        else:
            metric_type = MNB.MetricType(all_inputs[MNB.METRIC_TYPE_DROPDOWN])
            metric = self.create_metric_from_components(
                discovered_project, metric_type, all_inputs
            )
        return metric, metric_type

    def handle_input_changes(
        self, all_inputs: Dict[str, Any], ctx_triggered_id: str
    ) -> Tuple[List[html.Div], List[html.Div], str, str, str]:
        parse_result = urlparse(all_inputs[MITZU_LOCATION])
        project_name = self.get_path_project_name(parse_result)
        discovered_project = self.get_discovered_project(project_name)

        if discovered_project is None:
            def_mc_comp_children = MC.from_metric(None, None).children
            return (
                [],
                [c.to_plotly_json() for c in def_mc_comp_children],
                "?" + parse_result.query[2:],
                parse_result.geturl(),
                MNB.MetricType.SEGMENTATION.value,
            )

        metric, metric_type = self.metric_from_all_inputs(
            parse_result=parse_result,
            discovered_project=discovered_project,
            all_inputs=all_inputs,
            ctx_triggered_id=ctx_triggered_id,
        )

        url_params = "?"
        url = f"{parse_result.scheme}://{parse_result.netloc}{parse_result.path}"
        if metric is not None:
            url_params = "?m=" + quote(SE.to_compressed_string(metric))
            url = url + url_params

        metric_segments = MS.from_metric(
            discovered_project=discovered_project,
            metric=metric,
            metric_type=metric_type,
        ).children

        mc_children = MC.from_metric(metric, discovered_project).children
        return (
            metric_segments,
            mc_children,
            url_params,
            url,
            metric_type.value,
        )

    def create_callbacks(self):
        GH.create_callbacks(self)
        TH.create_callbacks(self)
        SS.create_callbacks(self)

        @self.app.callback(
            output=[
                Output(MS.METRIC_SEGMENTS, "children"),
                Output(MC.METRICS_CONFIG_CONTAINER, "children"),
                Output(MITZU_LOCATION, "search"),
                Output(MN.CLIPBOARD, "content"),
                Output(MNB.METRIC_TYPE_DROPDOWN, "value"),
            ],
            inputs=ALL_INPUT_COMPS,
            prevent_initial_call=True,
        )
        def on_inputs_change(
            all_inputs: Dict[str, Any],
        ) -> Tuple[List[html.Div], List[html.Div], str, str, str]:
            ctx_triggered_id = ctx.triggered_id
            LOGGER.debug(f"Inputs changed caused by: {ctx_triggered_id}")
            all_inputs = get_final_all_inputs(all_inputs, ctx.inputs_list)
            LOGGER.debug(all_inputs)
            return self.handle_input_changes(all_inputs, ctx_triggered_id)
