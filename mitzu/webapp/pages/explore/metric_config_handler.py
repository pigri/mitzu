from __future__ import annotations

from io import UnsupportedOperation
from typing import Any, Dict, List, Optional, Tuple

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
from mitzu.helper import parse_datetime_input

import mitzu.webapp.pages.explore.dates_selector_handler as DS
import mitzu.webapp.pages.explore.complex_segment_handler as CS
import mitzu.webapp.pages.explore.metric_type_handler as MTH
import mitzu.webapp.pages.explore.toolbar_handler as TH
from dash import html, ctx
import dash_mantine_components as dmc
from mitzu.webapp.helper import (
    CHILDREN,
    METRIC_SEGMENTS,
    find_event_field_def,
    value_to_label,
)

METRICS_CONFIG_CONTAINER = "metrics_config_container"

TIME_WINDOW = "time_window"
TIME_WINDOW_INTERVAL = "time_window_interval"
TIME_WINDOW_INTERVAL_STEPS = "time_window_interval_steps"
AGGREGATION_TYPE = "aggregation_type"
RESOLUTION_DD = "resolution_dd"
RESOLUTION_IG = "resolution_ig"


SUPPORTED_PERCENTILES = [50, 75, 90, 95, 99, 0, 100]


def agg_type_to_str(agg_type: M.AggType, agg_param: Any = None) -> str:
    if agg_type == M.AggType.CONVERSION:
        return "Conversion Rate"
    if agg_type == M.AggType.COUNT_EVENTS:
        return "Event Count"
    if agg_type == M.AggType.RETENTION_RATE:
        return "Retention Rate"
    if agg_type == M.AggType.COUNT_UNIQUE_USERS:
        return "User Count"
    if agg_type == M.AggType.AVERAGE_TIME_TO_CONV:
        return "Average Time To Convert"
    if agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
        if agg_param is None:
            raise ValueError("Time to convert metrics require an argument parameter")
        p_val = round(agg_param)
        if p_val == 50:
            return "Median Time To Convert"
        if p_val == 0:
            return "Min Time To Convert"
        if p_val == 100:
            return "Max Time To Convert"
        return f"P{p_val} Time To Convert"
    raise ValueError(f"Unsupported aggregation type {agg_type}")


def get_time_window_options(metric_type: MTH.MetricType) -> List[Dict[str, int]]:
    res: List[Dict[str, Any]] = []
    if metric_type == MTH.MetricType.CONVERSION:
        for tg in M.TimeGroup:
            if tg in (M.TimeGroup.TOTAL, M.TimeGroup.QUARTER):
                continue
            res.append({"label": tg.name.lower().title(), "value": tg.value})
    elif metric_type == MTH.MetricType.RETENTION:
        for tg in [
            M.TimeGroup.DAY,
            M.TimeGroup.WEEK,
            M.TimeGroup.MONTH,
            M.TimeGroup.YEAR,
        ]:
            if tg in (M.TimeGroup.TOTAL, M.TimeGroup.QUARTER):
                continue
            res.append({"label": tg.name.lower().title(), "value": tg.value})

    return res


def get_agg_type_options(metric_type: MTH.MetricType) -> List[Dict[str, str]]:
    if metric_type == MTH.MetricType.CONVERSION:
        res: List[Dict[str, Any]] = [
            {
                "label": agg_type_to_str(M.AggType.CONVERSION),
                "value": M.AggType.CONVERSION.to_agg_str(),
            },
            {
                "label": agg_type_to_str(M.AggType.AVERAGE_TIME_TO_CONV),
                "value": M.AggType.AVERAGE_TIME_TO_CONV.to_agg_str(),
            },
        ]
        res.extend(
            [
                {
                    "label": agg_type_to_str(M.AggType.PERCENTILE_TIME_TO_CONV, val),
                    "value": M.AggType.PERCENTILE_TIME_TO_CONV.to_agg_str(val),
                }
                for val in SUPPORTED_PERCENTILES
            ]
        )

        return res
    elif metric_type == MTH.MetricType.RETENTION:
        res = [
            {
                "label": agg_type_to_str(M.AggType.RETENTION_RATE),
                "value": M.AggType.RETENTION_RATE.to_agg_str(),
            }
        ]
        return res
    else:
        return [
            {
                "label": agg_type_to_str(M.AggType.COUNT_UNIQUE_USERS),
                "value": M.AggType.COUNT_UNIQUE_USERS.to_agg_str(),
            },
            {
                "label": agg_type_to_str(M.AggType.COUNT_EVENTS),
                "value": M.AggType.COUNT_EVENTS.to_agg_str(),
            },
        ]


def create_metric_options_component(
    metric_config: M.MetricConfig,
    metric_type: MTH.MetricType,
    time_window: M.TimeWindow,
) -> bc.Component:
    if metric_type == MTH.MetricType.SEGMENTATION:
        tw_value = 1
        tw_g_value = M.TimeGroup.DAY
        agg_type = metric_config.agg_type
        agg_param = metric_config.agg_param
        if agg_type not in (M.AggType.COUNT_UNIQUE_USERS, M.AggType.COUNT_EVENTS):
            agg_type = M.AggType.COUNT_UNIQUE_USERS
    elif metric_type == MTH.MetricType.CONVERSION:
        tw_value = time_window.value
        tw_g_value = time_window.period
        agg_type = metric_config.agg_type
        agg_param = metric_config.agg_param
        if agg_type not in (
            M.AggType.PERCENTILE_TIME_TO_CONV,
            M.AggType.AVERAGE_TIME_TO_CONV,
            M.AggType.CONVERSION,
        ):
            agg_type = M.AggType.CONVERSION
    elif metric_type == MTH.MetricType.RETENTION:
        tw_value = time_window.value
        tw_g_value = time_window.period
        if tw_g_value not in [
            M.TimeGroup.DAY,
            M.TimeGroup.WEEK,
            M.TimeGroup.MONTH,
            M.TimeGroup.YEAR,
        ]:
            tw_g_value = M.TimeGroup.WEEK

        agg_type = M.AggType.RETENTION_RATE
        agg_param = None
    else:
        agg_type = M.AggType.COUNT_UNIQUE_USERS
        agg_param = None
        tw_value = 1
        tw_g_value = M.TimeGroup.DAY

    aggregation_comp = dmc.Select(
        id=AGGREGATION_TYPE,
        label="Aggregation",
        className=AGGREGATION_TYPE + " rounded-right",
        clearable=False,
        value=M.AggType.to_agg_str(agg_type, agg_param),
        size="xs",
        data=get_agg_type_options(metric_type),
        style={
            "width": "204px",
        },
    )

    tw_label = (
        "Retention Period" if metric_type == MTH.MetricType.RETENTION else "Within"
    )

    time_window = html.Div(
        [
            dmc.NumberInput(
                id=TIME_WINDOW_INTERVAL,
                label=tw_label,
                className="me-1",
                type="number",
                max=10000,
                min=1,
                value=tw_value,
                size="xs",
                style={"width": "100px", "display": "inline-block"},
            ),
            dmc.Select(
                id=TIME_WINDOW_INTERVAL_STEPS,
                clearable=False,
                value=tw_g_value.value,
                size="xs",
                data=get_time_window_options(metric_type),
                style={"width": "100px", "display": "inline-block"},
            ),
        ],
        style={
            "visibility": "visible"
            if metric_type in [MTH.MetricType.RETENTION, MTH.MetricType.CONVERSION]
            else "hidden"
        },
    )

    resolution_ig = dmc.Select(
        id=RESOLUTION_DD,
        className="rounded-right",
        clearable=False,
        value=metric_config.resolution.name,
        size="xs",
        label="Resolution",
        data=[{"label": value_to_label(v.name), "value": v.name} for v in M.Resolution],
        style={
            "width": "204px",
            "visibility": (
                "visible"
                if metric_type in [MTH.MetricType.RETENTION, MTH.MetricType.CONVERSION]
                else "hidden"
            ),
        },
    )

    return html.Div(children=[aggregation_comp, time_window, resolution_ig])


def from_metric(
    metric_config: M.MetricConfig,
    metric_type: MTH.MetricType,
    time_window: M.TimeWindow,
) -> bc.Component:
    conversion_comps = [
        create_metric_options_component(metric_config, metric_type, time_window)
    ]
    component = dbc.Row(
        [
            dbc.Col(
                children=[DS.from_metric(metric_config, metric_type)],
                xs=12,
                md=6,
            ),
            dbc.Col(children=conversion_comps, xs=12, md=6),
        ],
        id=METRICS_CONFIG_CONTAINER,
        className=METRICS_CONFIG_CONTAINER,
    )
    return component


def from_all_inputs(
    discovered_project: Optional[M.DiscoveredProject],
    all_inputs: Dict[str, Any],
    metric_type: MTH.MetricType,
) -> Tuple[M.MetricConfig, M.TimeWindow]:
    agg_type_val = all_inputs.get(AGGREGATION_TYPE)
    if agg_type_val is None:
        if metric_type == M.MetricType.CONVERSION:
            agg_type, agg_param = M.AggType.CONVERSION, None
        elif metric_type == M.MetricType.SEGMENTATION:
            agg_type, agg_param = M.AggType.COUNT_UNIQUE_USERS, None
        elif metric_type == M.MetricType.RETENTION:
            agg_type, agg_param = M.AggType.RETENTION_RATE, None
        else:
            raise UnsupportedOperation(f"Unsupported Metric Type : {metric_type}")
    else:
        agg_type, agg_param = M.AggType.parse_agg_str(agg_type_val)

    if ctx.triggered_id == MTH.METRIC_TYPE_DROPDOWN:
        start_dt = None
        end_dt = None
        if metric_type == MTH.MetricType.RETENTION:
            time_group = M.TimeGroup.WEEK
            resolution = M.Resolution.ONE_USER_EVENT_PER_HOUR
            lookback_days = M.TimeWindow(2, M.TimeGroup.MONTH)
            time_window = M.TimeWindow(1, M.TimeGroup.WEEK)
        elif metric_type == MTH.MetricType.CONVERSION:
            time_group = M.TimeGroup.DAY
            resolution = M.Resolution.ONE_USER_EVENT_PER_MINUTE
            lookback_days = M.TimeWindow(1, M.TimeGroup.MONTH)
            time_window = M.TimeWindow(1, M.TimeGroup.DAY)
        else:
            time_group = M.TimeGroup.DAY
            resolution = M.Resolution.EVERY_EVENT
            lookback_days = M.TimeWindow(1, M.TimeGroup.MONTH)
            time_window = M.TimeWindow(1, M.TimeGroup.DAY)
    else:
        dates_conf = DS.from_all_inputs(discovered_project, all_inputs)
        time_group = dates_conf.time_group
        resolution = M.Resolution.parse(
            all_inputs.get(RESOLUTION_DD, M.Resolution.EVERY_EVENT.name)
        )
        lookback_days = dates_conf.lookback_days
        start_dt = dates_conf.start_dt
        end_dt = dates_conf.end_dt
        time_window = M.TimeWindow(
            value=all_inputs.get(TIME_WINDOW_INTERVAL, 1),
            period=M.TimeGroup(
                all_inputs.get(TIME_WINDOW_INTERVAL_STEPS, M.TimeGroup.DAY)
            ),
        )

    chart_type_val = all_inputs.get(TH.CHART_TYPE_DD, None)
    chart_type = M.SimpleChartType.parse(chart_type_val)

    group_by: Optional[M.EventFieldDef] = None
    if discovered_project is not None:
        group_by_paths = all_inputs[METRIC_SEGMENTS][CHILDREN]
        if len(group_by_paths) >= 1 and not (
            metric_type == MTH.MetricType.RETENTION and time_group != M.TimeGroup.TOTAL
        ):
            gp = group_by_paths[0].get(CS.COMPLEX_SEGMENT_GROUP_BY)
            group_by = find_event_field_def(gp, discovered_project) if gp else None
            if group_by is not None:
                group_by._project._discovered_project.set_value(discovered_project)

    res_config = M.MetricConfig(
        start_dt=parse_datetime_input(start_dt, None),
        end_dt=parse_datetime_input(end_dt, None),
        lookback_days=lookback_days,
        time_group=time_group,
        agg_type=agg_type,
        agg_param=agg_param,
        chart_type=chart_type,
        resolution=resolution,
        group_by=group_by,
        custom_title="",
    )
    return res_config, time_window
