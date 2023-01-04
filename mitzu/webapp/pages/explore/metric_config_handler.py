from __future__ import annotations

from io import UnsupportedOperation
from typing import Any, Dict, List, Optional, Tuple

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M

import mitzu.webapp.pages.explore.dates_selector_handler as DS
import mitzu.webapp.pages.explore.metric_type_handler as MTH
import mitzu.webapp.pages.explore.toolbar_handler as TH
from dash import html
import dash_mantine_components as dmc

METRICS_CONFIG_CONTAINER = "metrics_config_container"

TIME_WINDOW = "time_window"
TIME_WINDOW_INTERVAL = "time_window_interval"
TIME_WINDOW_INTERVAL_STEPS = "time_window_interval_steps"
AGGREGATION_TYPE = "aggregation_type"
RESOLUTION_DD = "resolution_dd"
RESOLUTION_IG = "resolution_ig"

EVERY_EVENT_RESOLUTION = "EVERY_EVENT"

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


def get_time_group_options() -> List[Dict[str, int]]:
    res: List[Dict[str, Any]] = []
    for tg in M.TimeGroup:
        if tg in (M.TimeGroup.TOTAL, M.TimeGroup.QUARTER):
            continue
        res.append({"label": tg.name.lower().title(), "value": tg.value})
    return res


def get_agg_type_options(metric: Optional[M.Metric]) -> List[Dict[str, str]]:
    if isinstance(metric, M.ConversionMetric):
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
    elif isinstance(metric, M.RetentionMetric):
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


def create_metric_options_component(metric: Optional[M.Metric]) -> bc.Component:

    if isinstance(metric, M.SegmentationMetric):
        tw_value = 1
        tg_value = M.TimeGroup.DAY
        agg_type = metric._agg_type
        agg_param = metric._agg_param
        if agg_type not in (M.AggType.COUNT_UNIQUE_USERS, M.AggType.COUNT_EVENTS):
            agg_type = M.AggType.COUNT_UNIQUE_USERS
    elif isinstance(metric, M.ConversionMetric):
        tw_value = metric._conv_window.value
        tg_value = metric._conv_window.period
        agg_type = metric._agg_type
        agg_param = metric._agg_param
        if agg_type not in (
            M.AggType.PERCENTILE_TIME_TO_CONV,
            M.AggType.AVERAGE_TIME_TO_CONV,
            M.AggType.CONVERSION,
        ):
            agg_type = M.AggType.CONVERSION
    elif isinstance(metric, M.RetentionMetric):
        tw_value = metric._retention_window.value
        tg_value = metric._retention_window.period
        agg_type = M.AggType.RETENTION_RATE
        agg_param = None
    else:
        agg_type = M.AggType.COUNT_UNIQUE_USERS
        agg_param = None
        tw_value = 1
        tg_value = M.TimeGroup.DAY

    aggregation_comp = dmc.Select(
        id=AGGREGATION_TYPE,
        label="Aggregation",
        className=AGGREGATION_TYPE + " rounded-right",
        clearable=False,
        value=M.AggType.to_agg_str(agg_type, agg_param),
        size="xs",
        data=get_agg_type_options(metric),
        style={
            "width": "204px",
        },
    )

    tw_label = "Retention Period" if isinstance(metric, M.RetentionMetric) else "Within"

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
                value=tg_value.value,
                size="xs",
                data=get_time_group_options(),
                style={"width": "100px", "display": "inline-block"},
            ),
        ],
        style={
            "visibility": "visible"
            if isinstance(metric, M.ConversionMetric)
            or isinstance(metric, M.RetentionMetric)
            else "hidden"
        },
    )

    res_value = (
        EVERY_EVENT_RESOLUTION
        if metric is None or metric._resolution is None
        else str(metric._resolution).lower()
    )

    resolution_ig = dmc.Select(
        id=RESOLUTION_DD,
        className="rounded-right",
        clearable=False,
        value=res_value,
        size="xs",
        label="Resolution",
        data=[
            {"label": "Every event", "value": EVERY_EVENT_RESOLUTION},
            {"label": "Single user event hourly", "value": "hour"},
            {"label": "Single user event daily", "value": "day"},
        ],
        style={
            "width": "204px",
            "visibility": (
                "visible"
                if isinstance(metric, M.ConversionMetric)
                or isinstance(metric, M.RetentionMetric)
                else "hidden"
            ),
        },
    )

    return html.Div(children=[aggregation_comp, time_window, resolution_ig])


def from_metric(
    metric: Optional[M.Metric],
) -> bc.Component:
    conversion_comps = [create_metric_options_component(metric)]
    component = dbc.Row(
        [
            dbc.Col(
                children=[DS.from_metric(metric)],
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

    res_tw = M.TimeWindow(
        value=all_inputs.get(TIME_WINDOW_INTERVAL, 1),
        period=M.TimeGroup(all_inputs.get(TIME_WINDOW_INTERVAL_STEPS, M.TimeGroup.DAY)),
    )

    res_val = all_inputs.get(RESOLUTION_DD, EVERY_EVENT_RESOLUTION)
    resolution: Optional[M.TimeGroup] = None
    if res_val != EVERY_EVENT_RESOLUTION:
        resolution = M.TimeGroup.parse(res_val)

    chart_type_val = all_inputs.get(TH.CHART_TYPE_DD, None)
    chart_type = M.SimpleChartType.parse(chart_type_val)

    dates_conf = DS.from_all_inputs(discovered_project, all_inputs)
    res_config = M.MetricConfig(
        start_dt=dates_conf.start_dt,
        end_dt=dates_conf.end_dt,
        lookback_days=dates_conf.lookback_days,
        time_group=dates_conf.time_group,
        agg_type=agg_type,
        agg_param=agg_param,
        chart_type=chart_type,
        resolution=resolution,
    )
    return res_config, res_tw
