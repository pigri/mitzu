from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import dash.development.base_component as bc
import dash_mantine_components as dmc
import mitzu.webapp.pages.explore.metric_type_handler as MTH

import mitzu.model as M
from dash import html

DATE_SELECTOR_DIV = "date_selector"
TIME_GROUP_DROPDOWN = "timegroup_dropdown"
LOOKBACK_WINDOW_DROPDOWN = "lookback_window_dropdown"
CUSTOM_DATE_PICKER = "custom_date_picker"
CUSTOM_DATE_TW_VALUE = "_custom_date"

CUSTOM_OPTION = {
    "label": "Custom",
    "value": CUSTOM_DATE_TW_VALUE,
}

DEFAULT_LOOKBACK_DAY_OPTION = M.TimeWindow(1, M.TimeGroup.MONTH)
LOOKBACK_DAYS_OPTIONS = {
    "Last 24 hours": M.TimeWindow(1, M.TimeGroup.DAY),
    "Last 7 days": M.TimeWindow(7, M.TimeGroup.DAY),
    "Last 14 days": M.TimeWindow(14, M.TimeGroup.DAY),
    "Last month": M.TimeWindow(1, M.TimeGroup.MONTH),
    "Last 2 month": M.TimeWindow(2, M.TimeGroup.MONTH),
    "Last 4 months": M.TimeWindow(4, M.TimeGroup.MONTH),
    "Last 6 months": M.TimeWindow(6, M.TimeGroup.MONTH),
    "Last 12 months": M.TimeWindow(12, M.TimeGroup.MONTH),
    "Last 2 years": M.TimeWindow(24, M.TimeGroup.MONTH),
    "Last 3 years": M.TimeWindow(36, M.TimeGroup.MONTH),
}


def get_time_group_options(exclude: List[M.TimeGroup]) -> List[Dict[str, Any]]:
    return [
        {"label": M.TimeGroup.group_by_string(tg), "value": tg.value}
        for tg in M.TimeGroup
        if tg not in exclude
    ]


def create_timewindow_options() -> List[Dict[str, str]]:
    return [{"label": k, "value": str(v)} for k, v in LOOKBACK_DAYS_OPTIONS.items()]


def get_default_tg_value(
    metric_config: M.MetricConfig, metric_type: MTH.MetricType
) -> M.TimeGroup:
    if metric_config is not None and metric_config.time_group is not None:
        return metric_config.time_group
    else:
        if metric_type == MTH.MetricType.RETENTION:
            return M.TimeGroup.WEEK
        else:
            return M.TimeGroup.DAY


def from_metric(
    metric_config: M.MetricConfig, metric_type: MTH.MetricType
) -> bc.Component:
    tg_val = get_default_tg_value(metric_config, metric_type)
    tw_options = create_timewindow_options()

    start_date = None
    end_date = None

    if metric_config is not None:
        if metric_config.start_dt is None and metric_config.lookback_days is not None:
            if metric_config.lookback_days in LOOKBACK_DAYS_OPTIONS.values():
                lookback_days = str(metric_config.lookback_days)
            else:
                lookback_days = str(DEFAULT_LOOKBACK_DAY_OPTION)
        elif metric_config.start_dt is not None:
            lookback_days = CUSTOM_DATE_TW_VALUE
        start_date = metric_config.start_dt
        end_date = metric_config.end_dt
    else:
        lookback_days = str(DEFAULT_LOOKBACK_DAY_OPTION)

    comp = html.Div(
        id=DATE_SELECTOR_DIV,
        children=[
            dmc.Select(
                id=TIME_GROUP_DROPDOWN,
                data=get_time_group_options(
                    exclude=[
                        M.TimeGroup.SECOND,
                        M.TimeGroup.MINUTE,
                        M.TimeGroup.QUARTER,
                    ]
                ),
                label="Period",
                value=tg_val.value,
                clearable=False,
                size="xs",
                style={"width": "120px"},
            ),
            html.Div(
                [
                    dmc.Select(
                        data=[CUSTOM_OPTION, *tw_options],
                        id=LOOKBACK_WINDOW_DROPDOWN,
                        className="me-1",
                        value=lookback_days,
                        clearable=False,
                        size="xs",
                        label="Time horizon",
                        searchable=True,
                        style={"width": "120px", "display": "inline-block"},
                    ),
                    dmc.DateRangePicker(
                        clearable=False,
                        inputFormat="YYYY-MM-DD",
                        id=CUSTOM_DATE_PICKER,
                        value=[start_date, end_date],
                        size="xs",
                        amountOfMonths=1,
                        style={
                            "width": "180px",
                            "display": "none"
                            if lookback_days != CUSTOM_DATE_TW_VALUE
                            else "inline-block",
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
    custom_dates = all_inputs.get(CUSTOM_DATE_PICKER, [None, None])
    start_dt = custom_dates[0]
    end_dt = custom_dates[1]
    lookback_days = all_inputs.get(LOOKBACK_WINDOW_DROPDOWN)

    if dd is None:
        return None, None

    if lookback_days == CUSTOM_DATE_TW_VALUE:
        if end_dt is None:
            end_dt = (
                dd.project.get_default_end_dt() if dd is not None else datetime.now()
            )
        if start_dt is None:
            def_lookback_window = (
                dd.project.webapp_settings.lookback_window.value
                if (
                    dd is not None
                    and dd.project.webapp_settings.lookback_window is not None
                )
                else 30
            )
            start_dt = end_dt - timedelta(days=def_lookback_window)
    else:
        return (None, None)
    return (start_dt, end_dt)


def get_metric_lookback_days(all_inputs: Dict[str, Any]) -> M.TimeWindow:
    tw_val = all_inputs.get(
        LOOKBACK_WINDOW_DROPDOWN,
    )
    if tw_val is None:
        return M.TimeWindow(1, M.TimeGroup.MONTH)
    if tw_val == CUSTOM_DATE_TW_VALUE:
        return M.DEF_LOOK_BACK_DAYS

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
