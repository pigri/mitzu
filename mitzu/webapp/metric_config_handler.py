from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.dates_selector_handler as DS
from dash import dcc, html
from mitzu.webapp.helper import find_first_component

METRICS_CONFIG_CONTAINER = "metrics_config_container"

CONVERSION_WINDOW = "conversion_window"
CONVERSION_WINDOW_INTERVAL = "conversion_window_interval"
CONVERSION_WINDOW_INTERVAL_STEPS = "conversion_window_interval_steps"
AGGREGATION_TYPE = "aggregation_type"

SUPPORTED_PERCENTILES = [50, 75, 90, 95, 99, 0, 100]


def agg_type_to_str(agg_type: M.AggType, agg_param: Any = None) -> str:
    if agg_type == M.AggType.CONVERSION:
        return "Conversion Rate"
    if agg_type == M.AggType.COUNT_EVENTS:
        return "Event Count"
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
    else:
        agg_type = M.AggType.COUNT_UNIQUE_USERS
        agg_param = None
        tw_value = 1
        tg_value = M.TimeGroup.DAY

    aggregation_comp = dbc.InputGroup(
        children=[
            dbc.InputGroupText("Aggregation", style={"width": "100px"}),
            dcc.Dropdown(
                id=AGGREGATION_TYPE,
                className=AGGREGATION_TYPE,
                clearable=False,
                multi=False,
                value=M.AggType.to_agg_str(agg_type, agg_param),
                options=get_agg_type_options(metric),
                style={
                    "width": "180px",
                    "border-radius": "0px 0.25rem 0.25rem 0px",
                },
            ),
        ],
    )
    conv_window = dbc.InputGroup(
        id=CONVERSION_WINDOW,
        children=[
            dbc.InputGroupText("Within", style={"width": "100px"}),
            dbc.Input(
                id=CONVERSION_WINDOW_INTERVAL,
                className=CONVERSION_WINDOW_INTERVAL,
                type="number",
                max=10000,
                min=1,
                value=tw_value,
                size="sm",
                style={"max-width": "60px"},
            ),
            dcc.Dropdown(
                id=CONVERSION_WINDOW_INTERVAL_STEPS,
                className=CONVERSION_WINDOW_INTERVAL_STEPS,
                clearable=False,
                multi=False,
                value=tg_value.value,
                options=get_time_group_options(),
                style={
                    "width": "121px",
                    "border-radius": "0px 0.25rem 0.25rem 0px",
                },
            ),
        ],
        style={
            "visibility": "visible"
            if isinstance(metric, M.ConversionMetric)
            else "hidden"
        },
    )

    return html.Div(children=[aggregation_comp, conv_window])


@dataclass
class MetricConfigHandler:

    component: bc.Component
    discovered_project: Optional[M.DiscoveredProject]

    @classmethod
    def from_component(
        cls,
        component: bc.Component,
        discovered_project: Optional[M.DiscoveredProject],
    ) -> MetricConfigHandler:
        return MetricConfigHandler(component, discovered_project)

    @classmethod
    def from_metric(
        cls,
        metric: Optional[M.Metric],
        discovered_project: Optional[M.DiscoveredProject],
    ) -> MetricConfigHandler:
        metric_config = metric._config if metric is not None else None
        conversion_comps = [create_metric_options_component(metric)]

        component = dbc.Row(
            [
                dbc.Col(
                    children=[
                        DS.DateSelectorHandler.from_metric_config(
                            metric_config, discovered_project
                        ).component
                    ],
                    xs=12,
                    md=6,
                ),
                dbc.Col(children=conversion_comps, xs=12, md=6),
            ],
            id=METRICS_CONFIG_CONTAINER,
            className=METRICS_CONFIG_CONTAINER,
        )

        return MetricConfigHandler(component, discovered_project)

    def to_metric_config_and_conv_window(
        self,
    ) -> Tuple[M.MetricConfig, Optional[M.TimeWindow]]:
        date_selector = find_first_component(DS.DATE_SELECTOR, self.component)
        c_steps = find_first_component(CONVERSION_WINDOW_INTERVAL_STEPS, self.component)
        c_interval = find_first_component(CONVERSION_WINDOW_INTERVAL, self.component)
        agg_type, agg_param = M.AggType.parse_agg_str(
            find_first_component(AGGREGATION_TYPE, self.component).value
        )
        res_tw: Optional[M.TimeWindow] = None

        if c_steps is not None:
            res_tw = M.TimeWindow(
                value=c_interval.value, period=M.TimeGroup(c_steps.value)
            )

        dates_conf = DS.DateSelectorHandler.from_component(
            date_selector, self.discovered_project
        ).to_metric_config()
        res_config = M.MetricConfig(
            start_dt=dates_conf.start_dt,
            end_dt=dates_conf.end_dt,
            lookback_days=dates_conf.lookback_days,
            time_group=dates_conf.time_group,
            agg_type=agg_type,
            agg_param=agg_param,
        )
        return res_config, res_tw
