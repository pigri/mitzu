from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import dash_bootstrap_components as dbc
import mitzu.model as M
from dash import dcc, html
from mitzu.webapp.helper import find_first_component

DATE_SELECTOR = "date_selector"
TIME_GROUP_DROPDOWN = "timegroup_dropdown"
LOOKBACK_WINDOW_DROPDOWN = "lookback_window_dropdown"
CUSTOM_DATE_PICKER = "custom_date_picker"
CUSTOM_DATE_TW_VALUE = -1
DEF_OPTIONS_COUNT = 5

CUSTOM_OPTION = {
    "label": html.Span(" Custom", className="bi bi-calendar-range"),
    "value": CUSTOM_DATE_TW_VALUE,
}

TW_MULTIPLIER: Dict[M.TimeGroup, int] = {
    M.TimeGroup.DAY: 30,
    M.TimeGroup.HOUR: 24,
    M.TimeGroup.MINUTE: 60,
    M.TimeGroup.SECOND: 60,
    M.TimeGroup.WEEK: 4,
    M.TimeGroup.MONTH: 3,
    M.TimeGroup.QUARTER: 1,
    M.TimeGroup.YEAR: 1,
}


def get_time_group_options(exclude: List[M.TimeGroup]) -> List[Dict[str, Any]]:
    return [
        {"label": M.TimeGroup.group_by_string(tg), "value": tg.value}
        for tg in M.TimeGroup
        if tg not in exclude
    ]


def create_timewindow_options(
    tg: M.TimeGroup, options_count: int = DEF_OPTIONS_COUNT
) -> List[Dict[str, Any]]:
    if tg == M.TimeGroup.TOTAL:
        tg = M.TimeGroup.DAY

    multiplier = TW_MULTIPLIER[tg]
    window = tg.name.lower().title()

    return [
        {"label": f"{i*multiplier} {window}", "value": i * multiplier}
        for i in range(1, options_count)
    ]


@dataclass
class DateSelectorHandler:

    component: html.Div
    discovered_project: Optional[M.DiscoveredProject]

    @classmethod
    def from_metric_config(
        cls,
        metric_config: Optional[M.MetricConfig],
        discovered_project: Optional[M.DiscoveredProject],
    ) -> DateSelectorHandler:
        tg_val = (
            metric_config.time_group
            if metric_config is not None and metric_config.time_group is not None
            else M.TimeGroup.DAY
        )
        tw_options = create_timewindow_options(tg_val)

        lookback_days = (
            TW_MULTIPLIER[tg_val]
            if tg_val != M.TimeGroup.TOTAL
            else TW_MULTIPLIER[M.TimeGroup.DAY]
        )
        start_date = None
        end_date = None

        if metric_config is not None:
            if (
                metric_config.start_dt is None
                and metric_config.lookback_days is not None
            ):
                if type(metric_config.lookback_days) == M.TimeWindow:
                    lookback_days = metric_config.lookback_days.value
                elif type(metric_config.lookback_days) == int:
                    lookback_days = metric_config.lookback_days
            elif metric_config.start_dt is not None:
                lookback_days = CUSTOM_DATE_TW_VALUE

            start_date = metric_config.start_dt
            end_date = metric_config.end_dt

        comp = html.Div(
            id=DATE_SELECTOR,
            children=[
                dbc.InputGroup(
                    children=[
                        dbc.InputGroupText("Period", style={"width": "60px"}),
                        dcc.Dropdown(
                            id=TIME_GROUP_DROPDOWN,
                            options=get_time_group_options(
                                exclude=[
                                    M.TimeGroup.SECOND,
                                    M.TimeGroup.MINUTE,
                                    M.TimeGroup.QUARTER,
                                ]
                            ),
                            value=tg_val.value,
                            clearable=False,
                            searchable=False,
                            multi=False,
                            style={
                                "width": "120px",
                                "border-radius": "0px 0.25rem 0.25rem 0px",
                            },
                        ),
                    ],
                ),
                dbc.InputGroup(
                    children=[
                        dbc.InputGroupText("Dates", style={"width": "60px"}),
                        dcc.Dropdown(
                            options=[*tw_options, CUSTOM_OPTION],
                            id=LOOKBACK_WINDOW_DROPDOWN,
                            value=lookback_days,
                            clearable=False,
                            searchable=False,
                            multi=False,
                            style={"width": "120px"},
                        ),
                        dcc.DatePickerRange(
                            clearable=True,
                            display_format="YYYY-MM-DD",
                            id=CUSTOM_DATE_PICKER,
                            className=CUSTOM_DATE_PICKER,
                            start_date=start_date,
                            end_date=end_date,
                            number_of_months_shown=1,
                            style={
                                "display": "none"
                                if lookback_days != CUSTOM_DATE_TW_VALUE
                                else "inline",
                            },
                        ),
                    ],
                ),
            ],
        )
        return DateSelectorHandler(
            component=comp, discovered_project=discovered_project
        )

    def get_metric_custom_dates(
        self, lookback_days: Optional[M.TimeWindow]
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        custom_date_picker = find_first_component(CUSTOM_DATE_PICKER, self.component)
        start_dt = custom_date_picker.start_date
        end_dt = custom_date_picker.end_date

        dd = self.discovered_project

        def_end_dt = (
            dd.project.default_end_dt
            if (dd is not None and dd.project.default_end_dt is not None)
            else datetime.now()
        )
        def_lookback_window = (
            dd.project.default_lookback_window.value
            if (dd is not None and dd.project.default_lookback_window is not None)
            else 30
        )
        if lookback_days is None:
            if end_dt is None:
                end_dt = def_end_dt
            if start_dt is None:
                start_dt = end_dt - timedelta(days=def_lookback_window)
        else:
            return (None, None)
        return (start_dt, end_dt)

    def get_metric_lookback_days(self) -> Optional[M.TimeWindow]:
        time_window_dd = find_first_component(LOOKBACK_WINDOW_DROPDOWN, self.component)
        time_group = self.get_metric_timegroup()

        options = create_timewindow_options(time_group)
        tw_val = time_window_dd.value

        if tw_val != CUSTOM_DATE_TW_VALUE:
            if tw_val not in [v["value"] for v in options]:
                if time_group == M.TimeGroup.TOTAL:
                    tw_val = TW_MULTIPLIER[M.TimeGroup.DAY]
                else:
                    tw_val = TW_MULTIPLIER[time_group]

            return M.TimeWindow(
                tw_val,
                time_group if time_group != M.TimeGroup.TOTAL else M.TimeGroup.DAY,
            )

        return None

    def get_metric_timegroup(self) -> M.TimeGroup:
        return M.TimeGroup(
            find_first_component(TIME_GROUP_DROPDOWN, self.component).value
        )

    @classmethod
    def from_component(
        cls,
        component: dbc.InputGroup,
        discovered_project: Optional[M.DiscoveredProject],
    ) -> DateSelectorHandler:
        return DateSelectorHandler(component, discovered_project)

    def to_metric_config(self) -> M.MetricConfig:
        lookback_days = self.get_metric_lookback_days()
        start_dt, end_dt = self.get_metric_custom_dates(lookback_days)
        time_group = self.get_metric_timegroup()

        return M.MetricConfig(
            start_dt=start_dt,
            end_dt=end_dt,
            lookback_days=lookback_days,
            time_group=time_group,
        )
