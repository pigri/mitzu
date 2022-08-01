from __future__ import annotations

import json
from typing import Dict, Optional

import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.visualization as VIZ
import pandas as pd
from dash import dcc, html
from mitzu.serialization import to_dict

GRAPH = "graph"
TABLE = "table"
SQL_AREA = "sql_area"
TABLE_COPY = "table_copy"

COPY_BUTTON_STYLE = {
    "display": "inline-block",
    "font-size": "24px",
    "position": "absolute",
    "top": "70px",
    "right": "50px",
}

CONTENT_STYLE = {
    "min-height": "500px",
    "max-height": "700px",
    "overflow": "auto",
    "font-size": "13px",
}

DF_CACHE: Dict[str, pd.DataFrame] = {}
MARKDOWN = """```sql
{sql}
```"""


def create_graph(metric: Optional[M.Metric]) -> dcc.Graph:
    if metric is not None:
        key = json.dumps(to_dict(metric))
        df = DF_CACHE.get(key)
        if df is None:
            df = metric.get_df()
            DF_CACHE[key] = df
    else:
        df = pd.DataFrame()

    if isinstance(metric, M.ConversionMetric):
        fig = VIZ.plot_conversion(metric, df)
    elif isinstance(metric, M.SegmentationMetric):
        fig = VIZ.plot_segmentation(metric, df)

    return dcc.Graph(
        id=GRAPH,
        figure=fig,
        config={"displayModeBar": False},
    )


def create_table(metric: Optional[M.Metric]) -> dbc.Table:
    if metric is not None:
        key = json.dumps(to_dict(metric))
        df = DF_CACHE.get(key)
        if df is None:
            df = metric.get_df()
            DF_CACHE[key] = df
    else:
        df = pd.DataFrame()

    df = df.sort_values(by=[df.columns[0], df.columns[1]])
    df.columns = [col[1:].replace("_", " ").title() for col in df.columns]
    table = dbc.Table.from_dataframe(
        df,
        id={"type": TABLE, "index": TABLE},
        striped=True,
        bordered=True,
        hover=True,
        size="sm",
    )
    return html.Div(
        id=GRAPH,
        children=[
            table,
            dcc.Clipboard(
                id={"type": TABLE_COPY, "index": TABLE_COPY},
                title="Copy",
                target_id={"type": TABLE, "index": TABLE},
                style=COPY_BUTTON_STYLE,
            ),
        ],
        style=CONTENT_STYLE,
    )


def create_sql_area(metric: Optional[M.Metric]) -> dbc.Table:
    if metric is not None:
        return html.Div(
            children=[
                dcc.Markdown(
                    children=MARKDOWN.format(sql=metric.get_sql()),
                    id=SQL_AREA,
                    style=CONTENT_STYLE,
                ),
                dcc.Clipboard(
                    target_id=SQL_AREA,
                    title="Copy",
                    style=COPY_BUTTON_STYLE,
                ),
            ]
        )
    else:
        return html.Div(id=SQL_AREA)
