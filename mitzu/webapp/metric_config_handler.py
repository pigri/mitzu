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


def get_time_group_options() -> List[Dict[str, int]]:
    res: List[Dict[str, Any]] = []
    for tg in M.TimeGroup:
        if tg in (M.TimeGroup.TOTAL, M.TimeGroup.QUARTER):
            continue
        res.append({"label": tg.name.lower().title(), "value": tg.value})
    return res


def create_conversion_component(metric: Optional[M.Metric]) -> bc.Component:
    if metric is None or not isinstance(metric, M.ConversionMetric):
        tw_value = 1
        tg_value = M.TimeGroup.DAY
    else:
        tw_value = metric._conv_window.value
        tg_value = metric._conv_window.period

    return html.Div(
        children=dbc.InputGroup(
            id=CONVERSION_WINDOW,
            children=[
                dbc.InputGroupText("Conversion Window:"),
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
                    style={"width": "120px"},
                ),
            ],
        ),
        style={
            "display": "block" if isinstance(metric, M.ConversionMetric) else "none"
        },
    )


@dataclass
class MetricConfigHandler:

    component: bc.Component
    discovered_datasource: Optional[M.DiscoveredEventDataSource]

    @classmethod
    def from_component(
        cls,
        component: bc.Component,
        discovered_datasource: Optional[M.DiscoveredEventDataSource],
    ) -> MetricConfigHandler:
        return MetricConfigHandler(component, discovered_datasource)

    @classmethod
    def from_metric(
        cls,
        metric: Optional[M.Metric],
        discovered_datasource: Optional[M.DiscoveredEventDataSource],
    ) -> MetricConfigHandler:
        metric_config = metric._config if metric is not None else None
        conversion_comps = [create_conversion_component(metric)]

        component = dbc.Card(
            children=[
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                children=[
                                    DS.DateSelectorHandler.from_metric_config(
                                        metric_config, discovered_datasource
                                    ).component
                                ],
                                xs=12,
                                md=6,
                            ),
                            dbc.Col(children=conversion_comps, xs=12, md=6),
                        ]
                    )
                )
            ],
            id=METRICS_CONFIG_CONTAINER,
            className=METRICS_CONFIG_CONTAINER,
        )
        return MetricConfigHandler(component, discovered_datasource)

    def to_metric_config_and_conv_window(
        self,
    ) -> Tuple[M.MetricConfig, Optional[M.TimeWindow]]:
        date_selector = find_first_component(DS.DATE_SELECTOR, self.component)
        c_steps = find_first_component(CONVERSION_WINDOW_INTERVAL_STEPS, self.component)
        c_interval = find_first_component(CONVERSION_WINDOW_INTERVAL, self.component)
        res_tw: Optional[M.TimeWindow] = None

        if c_steps is not None:
            res_tw = M.TimeWindow(
                value=c_interval.value, period=M.TimeGroup(c_steps.value)
            )

        dates_config = DS.DateSelectorHandler.from_component(
            date_selector, self.discovered_datasource
        ).to_metric_config()

        return dates_config, res_tw
