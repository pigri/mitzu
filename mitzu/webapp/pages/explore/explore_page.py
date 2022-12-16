from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union, cast
from urllib.parse import quote

import dash_bootstrap_components as dbc
import dash.development.base_component as bc

import mitzu.model as M
import mitzu.serialization as SE

import mitzu.webapp.pages.explore.complex_segment_handler as CS
import mitzu.webapp.pages.explore.dates_selector_handler as DS
import mitzu.webapp.pages.explore.event_segment_handler as ES
import mitzu.webapp.pages.explore.graph_handler as GH
import mitzu.webapp.pages.explore.metric_config_handler as MC
import mitzu.webapp.pages.explore.metric_segments_handler as MS
import mitzu.webapp.pages.explore.metric_type_handler as MNB
import mitzu.webapp.pages.explore.simple_segment_handler as SS
import mitzu.webapp.pages.explore.toolbar_handler as TH
import mitzu.webapp.navbar as NB
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.helper as H

import flask
from dash import ctx, html, callback, no_update, dcc
from dash.dependencies import ALL, Input, Output, State


from mitzu.helper import LOGGER
from mitzu.webapp.helper import (
    CHILDREN,
    METRIC_SEGMENTS,
    find_event_field_def,
    get_final_all_inputs,
)

NAVBAR_ID = "explore_navbar"
SHARE_BUTTON = "share_button"
CLIPBOARD = "share_clipboard"


EXPLORE_PAGE = "explore_page"
ALL_INPUT_COMPS = {
    "all_inputs": {
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
        MC.RESOLUTION_DD: Input(MC.RESOLUTION_DD, "value"),
        TH.GRAPH_REFRESH_BUTTON: Input(TH.GRAPH_REFRESH_BUTTON, "n_clicks"),
        TH.CHART_BUTTON: Input(TH.CHART_BUTTON, "n_clicks"),
        TH.TABLE_BUTTON: Input(TH.TABLE_BUTTON, "n_clicks"),
        TH.SQL_BUTTON: Input(TH.SQL_BUTTON, "n_clicks"),
    }
}


def create_navbar(metric: Optional[M.Metric], notebook_mode: bool) -> dbc.Navbar:
    navbar_children = [
        MNB.from_metric_type(MNB.MetricType.from_metric(metric)),
        dbc.Button(
            [
                html.B(className="bi bi-link-45deg"),
                " Share",
                dcc.Clipboard(
                    id=CLIPBOARD,
                    content="",
                    className="position-absolute start-0 top-0 w-100 h-100 opacity-0",
                ),
            ],
            id=SHARE_BUTTON,
            className="position-relative top-0 start-0 text-nowrap",
            color="light",
            size="sm",
            style={"display": "none" if notebook_mode else "inline-block"},
        ),
    ]

    return NB.create_mitzu_navbar(
        NAVBAR_ID,
        navbar_children,
        not notebook_mode,
    )


def create_explore_page(
    query_params: Dict[str, str],
    discovered_project: M.DiscoveredProject,
    notebook_mode: bool = False,
) -> bc.Component:
    if "m" in query_params:
        metric = SE.from_compressed_string(
            query_params["m"], discovered_project.project
        )
    else:
        metric = None

    metric_segments_div = MS.from_metric(metric, discovered_project)
    graph_container = create_graph_container(metric)
    navbar = create_navbar(metric, notebook_mode)
    res = html.Div(
        children=[
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
        className=EXPLORE_PAGE,
        id=EXPLORE_PAGE,
    )
    return res


def create_graph_container(metric: Optional[M.Metric]):
    metrics_config_card = MC.from_metric(metric)
    graph_handler = GH.create_graph_container()
    toolbar_handler = TH.create_toolbar_handler()

    graph_container = dbc.Card(
        children=[
            dbc.CardBody(
                children=[
                    metrics_config_card,
                    toolbar_handler,
                    graph_handler,
                ],
            ),
        ],
    )
    return graph_container


def create_metric_from_all_inputs(
    all_inputs: Dict[str, Any],
    discovered_project: M.DiscoveredProject,
) -> Optional[M.Metric]:
    segments = MS.from_all_inputs(discovered_project, all_inputs)
    metric_type = MNB.MetricType(all_inputs[MNB.METRIC_TYPE_DROPDOWN])
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
        agg_str = M.AggType.to_agg_str(metric_config.agg_type, metric_config.agg_param)
    else:
        agg_str = None

    group_by = None
    group_by_paths = all_inputs[METRIC_SEGMENTS][CHILDREN]
    if len(group_by_paths) >= 1 and not (
        metric_type == MNB.MetricType.RETENTION
        and metric_config.time_group != M.TimeGroup.TOTAL
    ):
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
            resolution=metric_config.resolution,
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
            resolution=metric_config.resolution,
            custom_title="",
            aggregation=agg_str,
        )

    return None


def handle_input_changes(
    all_inputs: Dict[str, Any], discovered_project: M.DiscoveredProject
) -> Tuple[List[html.Div], List[html.Div], str, str]:
    metric = create_metric_from_all_inputs(all_inputs, discovered_project)
    if metric is not None:
        url_params = "?m=" + quote(SE.to_compressed_string(metric))
    else:
        url_params = ""

    metric_segments = MS.from_metric(
        discovered_project=discovered_project,
        metric=metric,
    ).children

    mc_children = MC.from_metric(metric).children
    if metric is not None:
        metric_type_val = MNB.MetricType.from_metric(metric).value
    else:
        # This is the case when the url query is not parseable
        metric_type_val = all_inputs[MNB.METRIC_TYPE_DROPDOWN]

    return (metric_segments, mc_children, url_params, metric_type_val)


def create_callbacks():
    GH.create_callbacks()
    TH.create_callbacks()
    SS.create_callbacks()

    @callback(
        output=[
            Output(MS.METRIC_SEGMENTS, "children"),
            Output(MC.METRICS_CONFIG_CONTAINER, "children"),
            Output(H.MITZU_LOCATION, "search"),
            Output(CLIPBOARD, "content"),
            Output(MNB.METRIC_TYPE_DROPDOWN, "value"),
        ],
        inputs=ALL_INPUT_COMPS,
        state=dict(
            pathname=State(H.MITZU_LOCATION, "pathname"),
        ),
        prevent_initial_call=True,
    )
    def on_inputs_change(
        all_inputs: Dict[str, Any], pathname: str
    ) -> Tuple[List[html.Div], List[html.Div], str, str, str]:
        project_name = pathname.split("/")[-1]
        depenedencies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        discovered_project = depenedencies.storage.get_project(project_name)

        if discovered_project is None:
            return no_update, no_update, no_update, no_update, no_update  # noqa
        all_inputs = get_final_all_inputs(all_inputs, ctx.inputs_list)
        LOGGER.debug(all_inputs)
        segs, configs, url_params, type_dd_val = handle_input_changes(
            all_inputs, discovered_project
        )
        url = f"{flask.request.host_url}/{flask.request.path}{url_params}"
        return segs, configs, url_params, url, type_dd_val
