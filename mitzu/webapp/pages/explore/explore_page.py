from __future__ import annotations

from typing import Any, Dict, Optional, Union, cast
from urllib.parse import quote, urlparse

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
import mitzu.webapp.pages.paths as P
import flask

from dash import ctx, html, callback, no_update, dcc
from dash.dependencies import ALL, Input, Output, State


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
        DS.CUSTOM_DATE_PICKER: Input(DS.CUSTOM_DATE_PICKER, "value"),
        DS.LOOKBACK_WINDOW_DROPDOWN: Input(DS.LOOKBACK_WINDOW_DROPDOWN, "value"),
        MC.TIME_WINDOW_INTERVAL_STEPS: Input(MC.TIME_WINDOW_INTERVAL_STEPS, "value"),
        MC.TIME_WINDOW_INTERVAL: Input(MC.TIME_WINDOW_INTERVAL, "value"),
        MC.AGGREGATION_TYPE: Input(MC.AGGREGATION_TYPE, "value"),
        MC.RESOLUTION_DD: Input(MC.RESOLUTION_DD, "value"),
        TH.GRAPH_REFRESH_BUTTON: Input(TH.GRAPH_REFRESH_BUTTON, "n_clicks"),
        TH.CHART_TYPE_DD: Input(TH.CHART_TYPE_DD, "value"),
        TH.GRAPH_CONTENT_TYPE: Input(TH.GRAPH_CONTENT_TYPE, "value"),
    }
}


def create_navbar(
    metric: Optional[M.Metric],
    notebook_mode: bool,
    project_name: str,
) -> dbc.Navbar:
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
        off_canvas_toggler_visible=not notebook_mode,
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
    navbar = create_navbar(
        metric, notebook_mode, discovered_project.project.project_name
    )
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
    toolbar_handler = TH.from_metric(metric)

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
            chart_type=metric_config.chart_type,
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
            chart_type=metric_config.chart_type,
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
            chart_type=metric_config.chart_type,
            resolution=metric_config.resolution,
            custom_title="",
            aggregation=agg_str,
        )

    return None


def handle_input_changes(
    all_inputs: Dict[str, Any],
    discovered_project: M.DiscoveredProject,
) -> Dict[str, Any]:
    metric = create_metric_from_all_inputs(all_inputs, discovered_project)
    if metric is not None:
        url_params = "?m=" + quote(SE.to_compressed_string(metric))
    else:
        url_params = ""

    url = urlparse(all_inputs[H.MITZU_LOCATION])
    url = f"{url.scheme}://{url.hostname}/{url.path}{url_params}"

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

    chart_type_dd = TH.create_chart_type_dropdown(metric)

    return {
        MS.METRIC_SEGMENTS: metric_segments,
        MC.METRICS_CONFIG_CONTAINER: mc_children,
        CLIPBOARD: url,
        H.MITZU_LOCATION: url_params,
        MNB.METRIC_TYPE_DROPDOWN: metric_type_val,
        TH.CHART_TYPE_CONTAINER: chart_type_dd,
    }


def create_callbacks():
    GH.create_callbacks()
    SS.create_callbacks()

    @callback(
        output={
            MS.METRIC_SEGMENTS: Output(MS.METRIC_SEGMENTS, "children"),
            MC.METRICS_CONFIG_CONTAINER: Output(
                MC.METRICS_CONFIG_CONTAINER, "children"
            ),
            H.MITZU_LOCATION: Output(H.MITZU_LOCATION, "search"),
            CLIPBOARD: Output(CLIPBOARD, "content"),
            MNB.METRIC_TYPE_DROPDOWN: Output(MNB.METRIC_TYPE_DROPDOWN, "value"),
            TH.CHART_TYPE_CONTAINER: Output(TH.CHART_TYPE_CONTAINER, "children"),
        },
        inputs=ALL_INPUT_COMPS,
        state=dict(
            href=State(H.MITZU_LOCATION, "href"),
        ),
        prevent_initial_call=True,
    )
    def on_inputs_change(all_inputs: Dict[str, Any], href: str) -> Dict[str, Any]:
        url = urlparse(href)
        project_id = P.get_path_value(
            P.PROJECTS_EXPLORE_PATH, url.path, P.PROJECT_ID_PATH_PART
        )
        depenedencies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        discovered_project = depenedencies.storage.get_discovered_project(project_id)

        if discovered_project is None:
            return {
                MS.METRIC_SEGMENTS: no_update,
                MC.METRICS_CONFIG_CONTAINER: no_update,
                CLIPBOARD: no_update,
                H.MITZU_LOCATION: no_update,
                MNB.METRIC_TYPE_DROPDOWN: no_update,
                TH.CHART_TYPE_CONTAINER: no_update,
            }
        all_inputs = get_final_all_inputs(all_inputs, ctx.inputs_list)
        all_inputs[H.MITZU_LOCATION] = href

        return handle_input_changes(all_inputs, discovered_project)
