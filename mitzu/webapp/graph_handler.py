from __future__ import annotations

import os
import traceback
from typing import Any, Dict, Optional
from urllib.parse import urlparse
import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.toolbar_handler as TH
import mitzu.webapp.webapp as WA
import pandas as pd
from dash import Input, Output, State, ctx, dcc, html
from mitzu.webapp.helper import get_final_all_inputs
import mitzu.visualization.plot as PLT
import mitzu.visualization.charts as CHRT


GRAPH = "graph"
MESSAGE = "graph_message"
TABLE = "table"
SQL_AREA = "sql_area"


CONTENT_STYLE = {
    "min-height": "500px",
    "max-height": "700px",
    "overflow": "auto",
    "font-size": "13px",
}

DF_CACHE: Dict[int, pd.DataFrame] = {}
MARKDOWN = """```sql
{sql}
```"""

GRAPH_CONTAINER = "graph_container"
GRAPH_REFRESHER_INTERVAL = "graph_refresher_interval"
GRAPH_POLL_INTERVAL_MS = os.getenv("GRAPH_POLL_INTERVAL_MS", 200)


def create_graph_container() -> bc.Component:
    return html.Div(id=GRAPH_CONTAINER, children=[], className=GRAPH_CONTAINER)


def create_callbacks(webapp: WA.MitzuWebApp):
    background_callback = bool(
        os.getenv("BACKGROUND_CALLBACK", "true").lower() != "false"
    )

    @webapp.app.callback(
        output=Output(GRAPH_CONTAINER, "children"),
        inputs=WA.ALL_INPUT_COMPS,
        state=dict(
            chart_button_color=State(TH.CHART_BUTTON, "color"),
            table_button_color=State(TH.TABLE_BUTTON, "color"),
            sql_button_color=State(TH.SQL_BUTTON, "color"),
        ),
        interval=GRAPH_POLL_INTERVAL_MS,
        prevent_initial_call=True,
        background=background_callback,
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
    def handle_layout_changes_for_graph(
        all_inputs: Dict[str, Any],
        chart_button_color: str,
        table_button_color: str,
        sql_button_color: str,
    ) -> Any:

        try:
            all_inputs = get_final_all_inputs(all_inputs, ctx.inputs_list)
            parse_result = urlparse(all_inputs[WA.MITZU_LOCATION])
            project_name = webapp.get_path_project_name(parse_result)
            discovered_project = webapp.get_discovered_project(project_name)
            if discovered_project is None:
                return html.Div("First select a project", id=GRAPH, className=MESSAGE)
            metric, _ = webapp.metric_from_all_inputs(
                parse_result=parse_result,
                discovered_project=discovered_project,
                all_inputs=all_inputs,
                ctx_triggered_id=ctx.triggered_id,
            )
            if metric is None:
                return html.Div("Select an event", id=GRAPH, className=MESSAGE)

            if table_button_color == "info":
                return create_table(metric)
            elif sql_button_color == "info":
                return create_sql_area(metric)
            else:
                return create_graph(metric, webapp)

        except Exception as exc:
            traceback.print_exc()
            return html.Div(
                [
                    html.B("Something has gone wrong. Details:"),
                    html.Pre(children=str(exc)),
                ],
                id=GRAPH,
                style={"color": "red"},
            )


def create_graph(metric: M.Metric, webapp: WA.MitzuWebApp) -> Optional[dcc.Graph]:
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

    df = metric.get_df()

    if df is None:
        return None

    chart = CHRT.get_simple_chart(metric)
    fig = PLT.plot_chart(chart, metric)

    if webapp.results is not None:
        webapp.results["mitzu_df"] = df
        webapp.results["mitzu_fig"] = fig
    return dcc.Graph(id=GRAPH, figure=fig, config={"displayModeBar": False})


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
