from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.webapp as WA
from dash import ctx, dcc, html
from dash.dependencies import Input, Output, State
from mitzu.webapp.helper import deserialize_component, find_first_component

DATE_SELECTOR = "date_selector"
TIME_GROUP_DROWDOWN = "timegroup_dropdown"
LOOKBACK_WINDOW_DROPDOWN = "lookback_window_dropdown"
CUSTOM_DATE_PICKER = "custom_date_picker"
CUSTOM_DATE_TW_VALUE = -1

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
    tg: M.TimeGroup, options_count: int = 4
) -> List[Dict[str, Any]]:
    if tg == M.TimeGroup.TOTAL:
        tg = M.TimeGroup.DAY

    multiplier = TW_MULTIPLIER[tg]
    window = tg.name.lower().title()

    return [
        {"label": f"{i*multiplier} {window}", "value": i * multiplier}
        for i in range(1, options_count)
    ]


def create_date_selector():
    tw_options = create_timewindow_options(M.TimeGroup.DAY)
    return dbc.InputGroup(
        id=DATE_SELECTOR,
        children=[
            dbc.InputGroupText("Dates:"),
            dcc.Dropdown(
                id=TIME_GROUP_DROWDOWN,
                options=get_time_group_options(
                    exclude=[M.TimeGroup.SECOND, M.TimeGroup.MINUTE]
                ),
                value=M.TimeGroup.DAY.value,
                clearable=False,
                searchable=False,
                multi=False,
                style={"width": "120px", "height": "38px"},
            ),
            dcc.Dropdown(
                options=[*tw_options, CUSTOM_OPTION],
                id=LOOKBACK_WINDOW_DROPDOWN,
                value=tw_options[0]["value"],
                clearable=False,
                searchable=False,
                multi=False,
                style={"width": "120px", "height": "38px"},
            ),
            dcc.DatePickerRange(
                clearable=True,
                display_format="YYYY-MM-DD",
                id=CUSTOM_DATE_PICKER,
                className=CUSTOM_DATE_PICKER,
                start_date=None,
                end_date=None,
                number_of_months_shown=1,
                style={"display": "none"},
            ),
        ],
        style={"border-radius": "4px"},
    )


def get_metric_timegroup(date_selector: bc.Component) -> M.TimeGroup:
    return M.TimeGroup(find_first_component(TIME_GROUP_DROWDOWN, date_selector).value)


def get_metric_lookback_days(
    date_selector: bc.Component,
) -> Optional[M.TimeWindow]:
    time_window = find_first_component(LOOKBACK_WINDOW_DROPDOWN, date_selector).value
    time_group = get_metric_timegroup(date_selector)

    if time_window != CUSTOM_DATE_TW_VALUE:
        if time_group == M.TimeGroup.TOTAL:
            return M.TimeWindow(time_window, M.TimeGroup.DAY)
        return M.TimeWindow(time_window, time_group)

    return None


def get_metric_custom_dates(
    date_selector: bc.Component,
) -> Tuple[datetime, datetime]:
    custom_date_picker = find_first_component(CUSTOM_DATE_PICKER, date_selector)
    return (custom_date_picker.start_date, custom_date_picker.end_date)


def create_callbacks(webapp: WA.MitzuWebApp):
    @webapp.app.callback(
        Output(DATE_SELECTOR, "children"),
        Input(TIME_GROUP_DROWDOWN, "value"),
        Input(LOOKBACK_WINDOW_DROPDOWN, "value"),
        State(DATE_SELECTOR, "children"),
    )
    def input_changed(
        time_group_value: int,
        time_window_value: int,
        all_children: List[bc.Component],
    ) -> html.Div:
        children: List[bc.Component] = [
            deserialize_component(child) for child in all_children
        ]
        date_selector = find_first_component(CUSTOM_DATE_PICKER, children)
        if ctx.triggered_id == TIME_GROUP_DROWDOWN:
            tw_options = create_timewindow_options(M.TimeGroup(time_group_value))
            tw_dropdown = find_first_component(LOOKBACK_WINDOW_DROPDOWN, children)
            old_tw_dd_value = tw_dropdown.value
            tw_dropdown.options = [*tw_options, CUSTOM_OPTION]

            if old_tw_dd_value == CUSTOM_DATE_TW_VALUE:
                tw_dropdown.value = CUSTOM_DATE_TW_VALUE
                date_selector.style["display"] = "inline"
            else:
                tw_dropdown.value = tw_options[0]["value"]
                date_selector.style["display"] = "none"
        elif ctx.triggered_id == LOOKBACK_WINDOW_DROPDOWN:
            date_selector.style["display"] = (
                "none" if time_window_value != CUSTOM_DATE_TW_VALUE else "inline"
            )
        return [child.to_plotly_json() for child in children]
