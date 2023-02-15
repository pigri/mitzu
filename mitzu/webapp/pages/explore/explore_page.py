from __future__ import annotations

from typing import Any, Dict, Optional, Union, cast
from urllib.parse import quote, urlparse, parse_qs

import dash_bootstrap_components as dbc
import dash.development.base_component as bc

import mitzu.model as M
import mitzu.webapp.model as WM
import mitzu.serialization as SE
import mitzu.helper as H

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
import mitzu.visualization.charts as CHRT
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S
from mitzu.webapp.helper import MITZU_LOCATION
import mitzu.webapp.pages.paths as P
import flask
import dash_mantine_components as dmc
import traceback
from dash import ctx, html, callback, no_update, dcc
from dash.dependencies import ALL, Input, Output, State
from dash_iconify import DashIconify
from mitzu.webapp.auth.decorator import restricted


from mitzu.webapp.helper import (
    CHILDREN,
    METRIC_SEGMENTS,
    find_event_field_def,
    get_final_all_inputs,
)

NAVBAR_ID = "explore_navbar"
SHARE_BUTTON = "share_button"
CLIPBOARD = "share_clipboard"
METRIC_NAME_INPUT = "metric_name_input"
METRIC_ID_VALUE = "metric_id_value"
METRIC_NAME_PROGRESS = "metric_name_progress"
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
        TH.GRAPH_RUN_QUERY_BUTTON: Input(TH.GRAPH_RUN_QUERY_BUTTON, "n_clicks"),
        TH.CHART_TYPE_DD: Input(TH.CHART_TYPE_DD, "value"),
        TH.GRAPH_CONTENT_TYPE: Input(TH.GRAPH_CONTENT_TYPE, "value"),
    }
}


def create_navbar(
    metric: Optional[M.Metric],
    saved_metric: Optional[WM.SavedMetric],
    project: M.Project,
    notebook_mode: bool,
) -> dbc.Navbar:
    navbar_children = [
        MNB.from_metric_type(MNB.MetricType.from_metric(metric)),
        dmc.TextInput(
            id=METRIC_NAME_INPUT,
            debounce=700,
            placeholder="Name your metric",
            value=saved_metric.name if saved_metric is not None else None,
            size="xs",
            icon=DashIconify(icon="material-symbols:star", color="dark"),
            rightSection=dmc.Loader(
                size="xs",
                color="dark",
                className="d-none",
                id=METRIC_NAME_PROGRESS,
            ),
            style={"width": "200px"},
        ),
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
        id=NAVBAR_ID,
        children=navbar_children,
        off_canvas_toggler_visible=not notebook_mode,
        project_name=project.project_name,
    )


def create_explore_page(
    query_params: Dict[str, str],
    discovered_project: M.DiscoveredProject,
    storage: S.MitzuStorage,
    notebook_mode: bool = False,
) -> bc.Component:
    metric: Optional[M.Metric] = None
    saved_metric: Optional[WM.SavedMetric] = None
    if P.PROJECTS_EXPLORE_METRIC_QUERY in query_params:
        metric = SE.from_compressed_string(
            query_params[P.PROJECTS_EXPLORE_METRIC_QUERY], discovered_project.project
        )
    elif P.PROJECTS_EXPLORE_SAVED_METRIC_QUERY in query_params:
        saved_metric = storage.get_saved_metric(
            query_params[P.PROJECTS_EXPLORE_SAVED_METRIC_QUERY]
        )
        metric = saved_metric.metric

    metric_segments_div = MS.from_metric(metric, discovered_project)
    graph_container = create_graph_container(metric, discovered_project.project)
    navbar = create_navbar(
        metric=metric,
        saved_metric=saved_metric,
        project=discovered_project.project,
        notebook_mode=notebook_mode,
    )

    metric_id = H.create_unique_id() if saved_metric is None else saved_metric.id
    res = html.Div(
        children=[
            navbar,
            html.Div(children=metric_id, id=METRIC_ID_VALUE, className="d-none"),
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


def create_graph_container(metric: Optional[M.Metric], project: M.Project):
    metrics_config_card = MC.from_metric(metric)
    graph_handler = GH.create_graph_container()
    toolbar_handler = TH.from_metric(metric, project)

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
        if len(segments) >= 1:
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
        if group_by is not None:
            group_by._project._discovered_project.set_value(discovered_project)

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

    parse_result = urlparse(all_inputs[MITZU_LOCATION])
    if metric is not None:
        url_params = "m=" + quote(SE.to_compressed_string(metric))
        parse_result = parse_result._replace(query=url_params)
    url = parse_result.geturl()

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
    auto_refresh = discovered_project.project.webapp_settings.auto_refresh_enabled
    cancel_button_cls = (
        ""
        if (auto_refresh or ctx.triggered_id == TH.GRAPH_RUN_QUERY_BUTTON)
        else "d-none"
    )
    return {
        MS.METRIC_SEGMENTS: metric_segments,
        MC.METRICS_CONFIG_CONTAINER: mc_children,
        CLIPBOARD: url,
        MITZU_LOCATION: url,
        MNB.METRIC_TYPE_DROPDOWN: metric_type_val,
        TH.CHART_TYPE_CONTAINER: chart_type_dd,
        TH.CANCEL_BUTTON: cancel_button_cls,
    }


def create_callbacks():
    GH.create_callbacks()

    @callback(
        output={
            MS.METRIC_SEGMENTS: Output(MS.METRIC_SEGMENTS, "children"),
            MC.METRICS_CONFIG_CONTAINER: Output(
                MC.METRICS_CONFIG_CONTAINER, "children"
            ),
            MITZU_LOCATION: Output(MITZU_LOCATION, "href"),
            CLIPBOARD: Output(CLIPBOARD, "content"),
            MNB.METRIC_TYPE_DROPDOWN: Output(MNB.METRIC_TYPE_DROPDOWN, "value"),
            TH.CHART_TYPE_CONTAINER: Output(TH.CHART_TYPE_CONTAINER, "children"),
            TH.CANCEL_BUTTON: Output(TH.CANCEL_BUTTON, "class_name"),
        },
        inputs=ALL_INPUT_COMPS,
        state=dict(
            href=State(MITZU_LOCATION, "href"),
            metric_name=State(METRIC_NAME_INPUT, "value"),
            metric_id=State(METRIC_ID_VALUE, "children"),
        ),
        prevent_initial_call=True,
    )
    @restricted
    def on_inputs_change(
        all_inputs: Dict[str, Any],
        href: str,
        metric_name: Optional[str],
        metric_id: str,
    ) -> Dict[str, Any]:
        try:
            url = urlparse(href)
            project_id = P.get_path_value(
                P.PROJECTS_EXPLORE_PATH, url.path, P.PROJECT_ID_PATH_PART
            )
            depenedencies: DEPS.Dependencies = cast(
                DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
            )
            project = depenedencies.storage.get_project(project_id)
            if project is None:
                return {
                    MS.METRIC_SEGMENTS: no_update,
                    MC.METRICS_CONFIG_CONTAINER: no_update,
                    CLIPBOARD: no_update,
                    MITZU_LOCATION: no_update,
                    MNB.METRIC_TYPE_DROPDOWN: no_update,
                    TH.CHART_TYPE_CONTAINER: no_update,
                    TH.CANCEL_BUTTON: no_update,
                }
            all_inputs = get_final_all_inputs(all_inputs, ctx.inputs_list)
            all_inputs[METRIC_NAME_INPUT] = metric_name
            all_inputs[METRIC_ID_VALUE] = metric_id
            all_inputs[MITZU_LOCATION] = href
            discovered_project = project._discovered_project.get_value()
            if discovered_project is None:
                return {
                    MS.METRIC_SEGMENTS: no_update,
                    MC.METRICS_CONFIG_CONTAINER: no_update,
                    CLIPBOARD: no_update,
                    MITZU_LOCATION: no_update,
                    MNB.METRIC_TYPE_DROPDOWN: no_update,
                    TH.CHART_TYPE_CONTAINER: no_update,
                    TH.CANCEL_BUTTON: no_update,
                }
            return handle_input_changes(all_inputs, discovered_project)
        except Exception:
            traceback.print_exc()
            return {
                MS.METRIC_SEGMENTS: no_update,
                MC.METRICS_CONFIG_CONTAINER: no_update,
                CLIPBOARD: no_update,
                MITZU_LOCATION: no_update,
                MNB.METRIC_TYPE_DROPDOWN: no_update,
                TH.CHART_TYPE_CONTAINER: no_update,
                TH.CANCEL_BUTTON: no_update,
            }


@callback(
    Output(METRIC_NAME_PROGRESS, "className"),
    Input(METRIC_NAME_INPUT, "value"),
    State(METRIC_ID_VALUE, "children"),
    State(MITZU_LOCATION, "href"),
    background=True,
    running=[
        (Output(METRIC_NAME_PROGRESS, "className"), "d-inline-block", "d-none"),
    ],
    prevent_initial_call=True,
)
@restricted
def handle_metric_name_changed(
    metric_name: str,
    metric_id: str,
    href: str,
) -> str:
    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage

    parse_result = urlparse(href)
    sm = storage.get_saved_metric(metric_id)
    if sm is not None and sm.project is not None:
        storage.clear_saved_metric(metric_id)
        if metric_name:
            storage.set_saved_metric(metric_id, sm.rename(metric_name))
    else:

        project_id = P.get_path_value(
            P.PROJECTS_EXPLORE_PATH, parse_result.path, P.PROJECT_ID_PATH_PART
        )
        project = storage.get_project(project_id)
        metric_v64 = parse_qs(parse_result.query).get(P.PROJECTS_EXPLORE_METRIC_QUERY)
        if metric_v64 is not None:
            metric = SE.from_compressed_string(metric_v64[0], project)
            hash_key = GH.create_metric_hash_key(metric)
            result_df = GH.get_metric_result_df(hash_key, metric, storage)
            simple_chart = CHRT.get_simple_chart(metric, result_df)
            GH.store_rendered_saved_metric(
                metric_name=metric_name,
                metric=metric,
                simple_chart=simple_chart,
                project=project,
                storage=storage,
                metric_id=metric_id,
            )

    return "d-none"
