from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, cast
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
import mitzu.webapp.pages.explore.metric_type_handler as MTH
import mitzu.webapp.pages.explore.simple_segment_handler as SS
import mitzu.webapp.pages.explore.toolbar_handler as TH
import mitzu.webapp.navbar as NB
import mitzu.visualization.charts as CHRT
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S
from mitzu.webapp.helper import MITZU_LOCATION, find_event_field_def, CHILDREN
import mitzu.webapp.pages.paths as P
import flask
import traceback
from dash import ctx, html, callback, no_update, dcc
from dash.dependencies import ALL, Input, Output, State
from mitzu.webapp.auth.decorator import restricted


from mitzu.webapp.helper import (
    get_final_all_inputs,
)

NAVBAR_ID = "explore_navbar"
SHARE_BUTTON = "share_button"
CLIPBOARD = "share_clipboard"

METRIC_ID_VALUE = "metric_id_value"
METRIC_NAME_INPUT = "metric_name_input"
METRIC_SAVE_DIALOG = "metric_save_dialog"
METRIC_SAVE_NAVBAR_BUTTON = "metric_save_navbar_button"
METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON = "metric_save_dialog_save_new_button"
METRIC_SAVE_DIALOG_CLOSE_BUTTON = "metric_save_dialog_close_button"
METRIC_SAVE_DIALOG_REPLACE_BUTTON = "metric_save_dialog_replace_button"
METRIC_SAVE_DIALOG_INFO = "metric_save_dialog_info"

EXPLORE_PAGE = "explore_page"

ALL_INPUT_COMPS = {
    "all_inputs": {
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
        MTH.METRIC_TYPE_DROPDOWN: Input(MTH.METRIC_TYPE_DROPDOWN, "value"),
    }
}


def metric_type_navbar_provider(
    id: str, show_metric_type: bool = False, metric: Optional[M.Metric] = None, **kwargs
) -> Optional[bc.Component]:
    if not show_metric_type:
        return None
    return MTH.from_metric_type(MTH.MetricType.from_metric(metric))


def save_metric_navbar_provider(
    id: str,
    show_metric_name: bool = False,
    **kwargs,
) -> Optional[bc.Component]:
    if not show_metric_name:
        return None
    return dbc.Button(
        children=[html.I(className="bi bi-save me-1"), "save"],
        size="sm",
        color="success",
        id=METRIC_SAVE_NAVBAR_BUTTON,
    )


def share_button_navbar_provider(
    id: str, notebook_mode: bool = True, **kwargs
) -> Optional[bc.Component]:
    return (
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
    )


def create_saved_metric_dialog(saved_metric: Optional[WM.SavedMetric]) -> dbc.Modal:
    return dbc.Modal(
        [
            dbc.ModalHeader(
                (
                    "Save new metric"
                    if saved_metric is None
                    else "Save new or update metric"
                ),
                className="lead",
            ),
            dbc.ModalBody(
                [
                    dbc.Input(
                        value=saved_metric.name if saved_metric else "",
                        type="text",
                        minLength=4,
                        maxlength=30,
                        id=METRIC_NAME_INPUT,
                        placeholder="Metric name",
                    ),
                    html.Div(id=METRIC_SAVE_DIALOG_INFO, className="text-danger mt-1"),
                ],
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        [html.I(className=("bi bi-x me-1")), "Close"],
                        id=METRIC_SAVE_DIALOG_CLOSE_BUTTON,
                        size="sm",
                        color="secondary",
                        class_name="me-1",
                    ),
                    dbc.Button(
                        [
                            html.I(className=("bi bi-check me-1")),
                            ("Save" if saved_metric is None else "Save as new"),
                        ],
                        id=METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON,
                        size="sm",
                        color="success",
                    ),
                    dbc.Button(
                        [
                            html.I(className=("bi bi-arrow-counterclockwise me-1")),
                            "Update",
                        ],
                        id=METRIC_SAVE_DIALOG_REPLACE_BUTTON,
                        size="sm",
                        color="primary",
                        disabled=saved_metric is None,
                        class_name="d-none"
                        if saved_metric is None
                        else "d-inline-block",
                    ),
                ]
            ),
        ],
        id=METRIC_SAVE_DIALOG,
        is_open=False,
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
        try:
            metric = SE.from_compressed_string(
                query_params[P.PROJECTS_EXPLORE_METRIC_QUERY],
                discovered_project.project,
            )
        except Exception:
            traceback.print_exc()
            metric = None
    elif P.PROJECTS_EXPLORE_SAVED_METRIC_QUERY in query_params:
        saved_metric = storage.get_saved_metric(
            query_params[P.PROJECTS_EXPLORE_SAVED_METRIC_QUERY]
        )
        metric = saved_metric.metric

    metric_segments_div = MS.from_metric(metric, discovered_project)
    graph_container = create_graph_container(metric, discovered_project.project)
    save_metric_dialog = create_saved_metric_dialog(saved_metric)
    navbar = NB.create_mitzu_navbar(
        id=NAVBAR_ID,
        show_metric_type=True,
        show_metric_name=True,
        metric=metric,
        saved_metric=saved_metric,
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
            save_metric_dialog,
        ],
        className=EXPLORE_PAGE,
        id=EXPLORE_PAGE,
    )
    return res


def create_graph_container(metric: Optional[M.Metric], project: M.Project):
    metric_config = metric._config if metric is not None else M.MetricConfig()
    metric_type = MTH.MetricType.from_metric(metric)
    ret_or_conv_window = M.DEF_CONV_WINDOW
    if metric is not None:
        if isinstance(metric, M.ConversionMetric):
            ret_or_conv_window = metric._conv_window
        elif isinstance(metric, M.RetentionMetric):
            ret_or_conv_window = metric._retention_window
    else:
        if metric_type == MTH.MetricType.CONVERSION:
            ret_or_conv_window = M.DEF_CONV_WINDOW
        elif metric_type == MTH.MetricType.RETENTION:
            ret_or_conv_window = M.DEF_CONV_WINDOW

    metrics_config_card = MC.from_metric(metric_config, metric_type, ret_or_conv_window)
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


def get_metric_group_by(
    metric_config: M.MetricConfig,
    metric_type: MTH.MetricType,
    all_inputs: Dict[str, Any],
    discovered_project: M.DiscoveredProject,
):
    group_by = None
    group_by_paths = all_inputs[MS.METRIC_SEGMENTS][CHILDREN]
    if len(group_by_paths) >= 1 and not (
        metric_type == MTH.MetricType.RETENTION
        and metric_config.time_group != M.TimeGroup.TOTAL
    ):
        gp = group_by_paths[0].get(CS.COMPLEX_SEGMENT_GROUP_BY)
        group_by = find_event_field_def(gp, discovered_project) if gp else None
        if group_by is not None:
            group_by._event_data_table.project_reference = (
                M.Reference.create_from_value(discovered_project.project)
            )
    return group_by


def create_metric_from_all_inputs(
    all_inputs: Dict[str, Any],
    discovered_project: M.DiscoveredProject,
) -> Tuple[Optional[M.Metric], M.MetricConfig, M.TimeWindow]:
    segments = MS.from_all_inputs(discovered_project, all_inputs)
    metric_type = MTH.MetricType.parse(all_inputs[MTH.METRIC_TYPE_DROPDOWN])

    metric_config, time_window = MC.from_all_inputs(
        discovered_project, all_inputs, metric_type
    )
    metric: Optional[M.Metric] = None
    if metric_type == MTH.MetricType.CONVERSION:
        if len(segments) >= 1:
            metric = M.Conversion(segments)
    elif metric_type == MTH.MetricType.SEGMENTATION:
        if len(segments) == 1:
            metric = segments[0]
    elif metric_type == MTH.MetricType.RETENTION:
        if len(segments) == 2:
            metric = segments[0] >= segments[1]
        elif len(segments) == 1:
            metric = segments[0] >= segments[0]

    if metric is not None:
        metric = configure_metric(metric, metric_config, time_window)

    return metric, metric_config, time_window


def handle_input_changes(
    all_inputs: Dict[str, Any],
    discovered_project: M.DiscoveredProject,
) -> Dict[str, Any]:
    metric, metric_config, time_window = create_metric_from_all_inputs(
        all_inputs, discovered_project
    )
    parse_result = urlparse(all_inputs[MITZU_LOCATION])
    if metric is not None:

        url_params = "m=" + quote(SE.to_compressed_string(metric))
        parse_result = parse_result._replace(query=url_params)
    url = parse_result.geturl()

    metric_segments = MS.from_metric(
        discovered_project=discovered_project,
        metric=metric,
    ).children

    metric_type = all_inputs[MTH.METRIC_TYPE_DROPDOWN]

    mc_children = MC.from_metric(
        metric_config, MTH.MetricType.parse(metric_type), time_window
    ).children

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
        MTH.METRIC_TYPE_DROPDOWN: no_update,
        TH.CHART_TYPE_CONTAINER: chart_type_dd,
        TH.CANCEL_BUTTON: cancel_button_cls,
    }


def configure_metric(
    metric: M.Metric, metric_config: M.MetricConfig, time_window: M.TimeWindow
) -> M.Metric:
    if isinstance(metric, M.ConversionMetric):
        return M.ConversionMetric(
            conversion=metric._conversion,
            config=metric_config,
            conv_window=time_window,
        )

    if isinstance(metric, M.RetentionMetric):
        return M.RetentionMetric(
            initial_segment=metric._initial_segment,
            retaining_segment=metric._retaining_segment,
            retention_window=time_window,
            config=metric_config,
        )

    if isinstance(metric, M.SegmentationMetric):
        return M.SegmentationMetric(metric._segment, metric_config)

    raise Exception(f"Invalid metric type for graph visualization: {type(metric)}")


def create_callbacks():
    GH.create_callbacks()

    no_update_response = {
        MS.METRIC_SEGMENTS: no_update,
        MC.METRICS_CONFIG_CONTAINER: no_update,
        CLIPBOARD: no_update,
        MITZU_LOCATION: no_update,
        MTH.METRIC_TYPE_DROPDOWN: no_update,
        TH.CHART_TYPE_CONTAINER: no_update,
        TH.CANCEL_BUTTON: no_update,
    }

    @callback(
        output={
            MS.METRIC_SEGMENTS: Output(MS.METRIC_SEGMENTS, "children"),
            MC.METRICS_CONFIG_CONTAINER: Output(
                MC.METRICS_CONFIG_CONTAINER, "children"
            ),
            MITZU_LOCATION: Output(MITZU_LOCATION, "href"),
            CLIPBOARD: Output(CLIPBOARD, "content"),
            MTH.METRIC_TYPE_DROPDOWN: Output(MTH.METRIC_TYPE_DROPDOWN, "value"),
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
            if len(ctx.triggered_prop_ids) != 1:
                # Ignoring all components change as this comes from
                # page layout load and everything should be already rendered
                return no_update_response

            url = urlparse(href)
            project_id = P.get_path_value(
                P.PROJECTS_EXPLORE_PATH, url.path, P.PROJECT_ID_PATH_PART
            )
            depenedencies: DEPS.Dependencies = cast(
                DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
            )
            project = depenedencies.storage.get_project(project_id)
            if project is None:
                return no_update_response
            all_inputs = get_final_all_inputs(all_inputs, ctx.inputs_list)
            all_inputs[METRIC_NAME_INPUT] = metric_name
            all_inputs[METRIC_ID_VALUE] = metric_id
            all_inputs[MITZU_LOCATION] = href
            discovered_project = project._discovered_project.get_value()
            if discovered_project is None:
                return no_update_response
            return handle_input_changes(all_inputs, discovered_project)
        except Exception:
            traceback.print_exc()
            return no_update_response


@callback(
    Input(METRIC_SAVE_NAVBAR_BUTTON, "n_clicks"),
    Input(METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON, "n_clicks"),
    Input(METRIC_SAVE_DIALOG_REPLACE_BUTTON, "n_clicks"),
    Input(METRIC_SAVE_DIALOG_CLOSE_BUTTON, "n_clicks"),
    State(METRIC_ID_VALUE, "children"),
    State(METRIC_NAME_INPUT, "value"),
    State(MITZU_LOCATION, "href"),
    output={
        METRIC_SAVE_DIALOG: Output(METRIC_SAVE_DIALOG, "is_open"),
        METRIC_NAME_INPUT: Output(METRIC_NAME_INPUT, "value"),
        METRIC_SAVE_DIALOG_INFO: Output(METRIC_SAVE_DIALOG_INFO, "children"),
    },
    prevent_initial_call=True,
)
@restricted
def handle_save_metric_dialog(
    navbar_btn_nclicks: int,
    save_new_nclicks: int,
    replace_nclicks: int,
    close_nclicks: int,
    metric_id: str,
    metric_name: str,
    href: str,
) -> Dict[str, Any]:
    deps = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY))
    storage = deps.storage
    mitzu_cache = deps.cache
    if ctx.triggered_id == METRIC_SAVE_NAVBAR_BUTTON:
        original_name = None
        if metric_id is not None:
            try:
                sm = storage.get_saved_metric(metric_id)
                if sm is not None:
                    original_name = sm.name
            except ValueError:
                original_name = None

        return {
            METRIC_SAVE_DIALOG: True,
            METRIC_NAME_INPUT: original_name,
            METRIC_SAVE_DIALOG_INFO: "",
        }

    if ctx.triggered_id == METRIC_SAVE_DIALOG_CLOSE_BUTTON:
        return {
            METRIC_SAVE_DIALOG: False,
            METRIC_NAME_INPUT: None,
            METRIC_SAVE_DIALOG_INFO: "",
        }

    if not metric_name:
        return {
            METRIC_SAVE_DIALOG: no_update,
            METRIC_NAME_INPUT: no_update,
            METRIC_SAVE_DIALOG_INFO: "Please name your metric.",
        }
    if len(metric_name) < 4:
        return {
            METRIC_SAVE_DIALOG: no_update,
            METRIC_NAME_INPUT: no_update,
            METRIC_SAVE_DIALOG_INFO: "Metric name must be at least 4 characters.",
        }
    if len(metric_name) > 30:
        return {
            METRIC_SAVE_DIALOG: no_update,
            METRIC_NAME_INPUT: no_update,
            METRIC_SAVE_DIALOG_INFO: "Metric name must be at most 30 characters.",
        }

    try:
        parse_result = urlparse(href)

        project_id = P.get_path_value(
            P.PROJECTS_EXPLORE_PATH, parse_result.path, P.PROJECT_ID_PATH_PART
        )
        project = storage.get_project(project_id)
        metric_v64 = parse_qs(parse_result.query).get(P.PROJECTS_EXPLORE_METRIC_QUERY)
        if metric_v64 is not None:
            metric = SE.from_compressed_string(metric_v64[0], project)

        elif metric_id is not None:
            sm = storage.get_saved_metric(metric_id)
            if sm is None or sm.metric is None:
                return {
                    METRIC_SAVE_DIALOG: True,
                    METRIC_NAME_INPUT: no_update,
                    METRIC_SAVE_DIALOG_INFO: "Couldn't save metric. Invalid state.",
                }

            if (
                sm.name.strip() == metric_name.strip()
                and ctx.triggered_id == METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON
            ):
                return {
                    METRIC_SAVE_DIALOG: True,
                    METRIC_NAME_INPUT: no_update,
                    METRIC_SAVE_DIALOG_INFO: "Make sure you give a different name to your metric.",
                }
            metric = sm.metric
        else:
            return {
                METRIC_SAVE_DIALOG: True,
                METRIC_NAME_INPUT: no_update,
                METRIC_SAVE_DIALOG_INFO: "Couldn't save metric. Invalid state.",
            }
        hash_key = GH.create_metric_hash_key(metric)
        result_df = GH.get_metric_result_df(hash_key, metric, mitzu_cache)
        simple_chart = CHRT.get_simple_chart(metric, result_df)

        if ctx.triggered_id == METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON:
            metric_id = H.create_unique_id()

        GH.store_rendered_saved_metric(
            metric_name=metric_name,
            metric=metric,
            simple_chart=simple_chart,
            project=project,
            storage=storage,
            metric_id=metric_id,
        )
        return {
            METRIC_SAVE_DIALOG: False,
            METRIC_NAME_INPUT: no_update,
            METRIC_SAVE_DIALOG_INFO: "",
        }
    except Exception:
        traceback.print_exc()
        return {
            METRIC_SAVE_DIALOG: True,
            METRIC_NAME_INPUT: no_update,
            METRIC_SAVE_DIALOG_INFO: "Couldn't save metric. Something went wrong.",
        }
