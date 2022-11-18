from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
from dash import dcc, html

DATE_SELECTOR_DIV = "date_selector"
TIME_GROUP_DROPDOWN = "timegroup_dropdown"
LOOKBACK_WINDOW_DROPDOWN = "lookback_window_dropdown"
CUSTOM_DATE_PICKER = "custom_date_picker"
CUSTOM_DATE_PICKER_START_DATE = "custom_date_picker_start_date"
CUSTOM_DATE_PICKER_END_DATE = "custom_date_picker_end_date"
CUSTOM_DATE_TW_VALUE = "_custom_date"
DEF_OPTIONS_COUNT = 5

CUSTOM_OPTION = {
    "label": html.Span(" Custom", className="bi bi-calendar-range"),
    "value": CUSTOM_DATE_TW_VALUE,
}

DEFAULT_LOOKBACK_DAY_OPTION = M.TimeWindow(1, M.TimeGroup.MONTH)
LOOKBACK_DAYS_OPTIONS = {
    "Today": M.TimeWindow(0, M.TimeGroup.DAY),
    "Last 7 days": M.TimeWindow(7, M.TimeGroup.DAY),
    "Last 14 days": M.TimeWindow(14, M.TimeGroup.DAY),
    "Last month": M.TimeWindow(1, M.TimeGroup.MONTH),
    "Last 2 month": M.TimeWindow(2, M.TimeGroup.MONTH),
    "Last 4 months": M.TimeWindow(4, M.TimeGroup.MONTH),
    "Last 6 months": M.TimeWindow(6, M.TimeGroup.MONTH),
    "Last 12 months": M.TimeWindow(12, M.TimeGroup.MONTH),
    "Last 2 years": M.TimeWindow(2, M.TimeGroup.YEAR),
}


def get_time_group_options(exclude: List[M.TimeGroup]) -> List[Dict[str, Any]]:
    return [
        {"label": M.TimeGroup.group_by_string(tg), "value": tg.value}
        for tg in M.TimeGroup
        if tg not in exclude
    ]


def create_timewindow_options() -> List[Dict[str, str]]:
    return [{"label": k, "value": str(v)} for k, v in LOOKBACK_DAYS_OPTIONS.items()]


def get_default_tg_value(metric: Optional[M.Metric]) -> M.TimeGroup:
    metric_config = metric._config if metric is not None else None
    if metric is not None:
        if metric_config is not None and metric_config.time_group is not None:
            return metric_config.time_group
        else:
            if isinstance(metric, M.RetentionMetric):
                return M.TimeGroup.WEEK
            else:
                return M.TimeGroup.DAY
    else:
        return M.TimeGroup.DAY


def from_metric(
    metric: Optional[M.Metric],
) -> bc.Component:
    metric_config = metric._config if metric is not None else None
    tg_val = get_default_tg_value(metric)
    tw_options = create_timewindow_options()

    start_date = None
    end_date = None

    if metric_config is not None:
        if metric_config.start_dt is None and metric_config.lookback_days is not None:
            if type(metric_config.lookback_days) == M.TimeWindow:
                lookback_days = str(metric_config.lookback_days)
            else:
                raise Exception(
                    f"Unsupported lookback days type {type(metric_config.lookback_days)}"
                )
        elif metric_config.start_dt is not None:
            lookback_days = CUSTOM_DATE_TW_VALUE
        start_date = metric_config.start_dt
        end_date = metric_config.end_dt
    else:
        lookback_days = str(DEFAULT_LOOKBACK_DAY_OPTION)

    comp = html.Div(
        id=DATE_SELECTOR_DIV,
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
                        options=[CUSTOM_OPTION, *tw_options],
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
    return comp


def get_metric_custom_dates(
    dd: Optional[M.DiscoveredProject], all_inputs: Dict[str, Any]
) -> Tuple[Optional[datetime], Optional[datetime]]:
    start_dt = all_inputs.get(CUSTOM_DATE_PICKER_START_DATE)
    end_dt = all_inputs.get(CUSTOM_DATE_PICKER_END_DATE)
    lookback_days = all_inputs.get(LOOKBACK_WINDOW_DROPDOWN)

    if dd is None:
        return None, None

    if lookback_days == CUSTOM_DATE_TW_VALUE:
        if end_dt is None:
            end_dt = (
                dd.project.default_end_dt
                if (dd is not None and dd.project.default_end_dt is not None)
                else datetime.now()
            )
        if start_dt is None:
            def_lookback_window = (
                dd.project.default_lookback_window.value
                if (dd is not None and dd.project.default_lookback_window is not None)
                else 30
            )
            start_dt = end_dt - timedelta(days=def_lookback_window)
    else:
        return (None, None)
    return (start_dt, end_dt)


def get_metric_lookback_days(all_inputs: Dict[str, Any]) -> Optional[M.TimeWindow]:
    tw_val = all_inputs.get(
        LOOKBACK_WINDOW_DROPDOWN,
    )
    if tw_val is None:
        return M.TimeWindow(1, M.TimeGroup.MONTH)
    if tw_val == CUSTOM_DATE_TW_VALUE:
        return None

    return M.TimeWindow.parse(str(tw_val))


def from_all_inputs(
    discovered_project: Optional[M.DiscoveredProject], all_inputs: Dict[str, Any]
) -> M.MetricConfig:
    lookback_days = get_metric_lookback_days(all_inputs)
    start_dt, end_dt = get_metric_custom_dates(discovered_project, all_inputs)
    time_group = M.TimeGroup(all_inputs.get(TIME_GROUP_DROPDOWN))

    return M.MetricConfig(
        start_dt=start_dt,
        end_dt=end_dt,
        lookback_days=lookback_days,
        time_group=time_group,
    )
