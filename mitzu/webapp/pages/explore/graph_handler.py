from __future__ import annotations

import traceback
from typing import Any, Dict, Optional, cast
import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.pages.explore.toolbar_handler as TH
import mitzu.webapp.pages.explore.explore_page as EXP
import mitzu.webapp.configs as configs
import mitzu.webapp.dependencies as DEPS
import flask
import mitzu.webapp.pages.paths as P

import pandas as pd
from dash import Input, Output, State, ctx, dcc, html, callback, no_update
from mitzu.webapp.helper import get_final_all_inputs, MITZU_LOCATION
import mitzu.visualization.plot as PLT
import mitzu.visualization.charts as CHRT

GRAPH = "graph"
MESSAGE = "lead fw-normal text-center h-100 w-100"
TABLE = "table"
SQL_AREA = "sql_area"


CONTENT_STYLE = {
    "min-height": "200px",
    "max-height": "700px",
    "overflow": "auto",
}

DF_CACHE: Dict[int, pd.DataFrame] = {}
MARKDOWN = """```sql
{sql}
```"""

GRAPH_CONTAINER = "graph_container d-flex align-items-center justify-content-stretch"
GRAPH_REFRESHER_INTERVAL = "graph_refresher_interval"


def create_graph_container() -> bc.Component:
    if configs.BACKGROUND_CALLBACK:
        return html.Div(
            id=GRAPH_CONTAINER,
            children=[],
            className=GRAPH_CONTAINER,
        )
    else:
        return html.Div(
            dcc.Loading(id=GRAPH_CONTAINER, children=[], color="#287bb5", type="dot"),
            className=GRAPH_CONTAINER,
        )


def create_callbacks():
    @callback(
        output=Output(GRAPH_CONTAINER, "children"),
        inputs=EXP.ALL_INPUT_COMPS,
        state=dict(
            graph_content_type=State(TH.GRAPH_CONTENT_TYPE, "value"),
            pathname=State(MITZU_LOCATION, "pathname"),
        ),
        interval=configs.GRAPH_POLL_INTERVAL_MS,
        prevent_initial_call=True,
        background=configs.BACKGROUND_CALLBACK,
        running=[
            (
                Output(TH.GRAPH_REFRESH_BUTTON, "disabled"),
                True,
                False,
            ),
            (
                Output(TH.CANCEL_BUTTON, "style"),
                TH.VISIBLE,
                TH.HIDDEN,
            ),
            (
                Output(GRAPH_CONTAINER, "style"),
                {"opacity": "0.5"},
                {"opacity": "1"},
            ),
        ],
        cancel=[Input(TH.CANCEL_BUTTON, "n_clicks")],
    )
    def handle_changes_for_graph(
        all_inputs: Dict[str, Any],
        graph_content_type: str,
        pathname: str,
    ) -> bc.Component:
        project_id = P.get_path_value(
            P.PROJECTS_EXPLORE_PATH, pathname, P.PROJECT_ID_PATH_PART
        )
        depenedencies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        discovered_project = depenedencies.storage.get_discovered_project(project_id)

        if discovered_project is None:
            return no_update
        try:
            all_inputs = get_final_all_inputs(all_inputs, ctx.inputs_list)
            metric = EXP.create_metric_from_all_inputs(all_inputs, discovered_project)
            if metric is None:
                return html.Div("Select an event", id=GRAPH, className=MESSAGE)

            if graph_content_type == TH.TABLE_VAL:
                return create_table(metric)
            elif graph_content_type == TH.SQL_VAL:
                return create_sql_area(metric)

            return create_graph(metric)
        except Exception as exc:
            traceback.print_exc()
            return html.Div(
                [
                    html.B("Something has gone wrong. Details:"),
                    html.Pre(children=str(exc)),
                ],
                id=GRAPH,
                className="text-danger small",
            )


def create_graph(metric: M.Metric) -> Optional[dcc.Graph]:
    if metric is None:
        return html.Div("Select the first event...", id=GRAPH, className=MESSAGE)

    if (
        (isinstance(metric, M.SegmentationMetric) and metric._segment is None)
        or (
            isinstance(metric, M.ConversionMetric)
            and len(metric._conversion._segments) == 0
        )
        or (isinstance(metric, M.RetentionMetric) and metric._initial_segment is None)
    ):
        return html.Div("Select the first event...", id=GRAPH, className=MESSAGE)

    chart = CHRT.get_simple_chart(metric)
    fig = PLT.plot_chart(chart, metric)
    return dcc.Graph(
        id=GRAPH, className="w-100", figure=fig, config={"displayModeBar": False}
    )


def create_table(metric: M.Metric) -> Optional[dbc.Table]:
    if metric is None:
        return None

    df = metric.get_df()
    if df is None:
        return None

    df = df.sort_values(by=[df.columns[0], df.columns[1]])
    df.columns = [col[1:].replace("_", " ").title() for col in df.columns]

    table = dbc.Table.from_dataframe(
        df,
        id={"type": TABLE, "index": TABLE},
        striped=True,
        bordered=True,
        hover=True,
        size="sm",
        style=CONTENT_STYLE,
    )
    return table


def create_sql_area(metric: M.Metric) -> dbc.Table:
    if metric is not None:
        return dcc.Markdown(
            children=MARKDOWN.format(sql=metric.get_sql()),
            id=SQL_AREA,
        )
    else:
        return html.Div(id=SQL_AREA)
