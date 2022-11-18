from __future__ import annotations


import plotly.express as px
import plotly.figure_factory as ff
import pandas as pd
import mitzu.model as M
import mitzu.visualization.common as C
from typing import Dict, List


PRISM2 = [
    "rgb(95, 70, 144)",
    "rgb(237, 173, 8)",
    "rgb(29, 105, 150)",
    "rgb(225, 124, 5)",
    "rgb(56, 166, 165)",
    "rgb(204, 80, 62)",
    "rgb(15, 133, 84)",
    "rgb(148, 52, 110)",
    "rgb(115, 175, 72)",
    "rgb(111, 64, 112)",
    "rgb(102, 102, 102)",
]


def set_figure_style(fig, simple_chart: C.SimpleChart, metric: M.Metric):
    if simple_chart.title is not None:
        title_height = len(simple_chart.title.split("<br />")) * 30
    else:
        title_height = 0

    if metric._config.time_group != M.TimeGroup.TOTAL:
        fig.update_traces(
            line=dict(width=2.5),
            mode="lines+markers",
            textposition="top center",
            textfont_size=9,
        )
    else:
        fig.update_traces(textposition="outside", textfont_size=9)

    fig.update_yaxes(
        rangemode="tozero",
        showline=True,
        linecolor="rgba(127,127,127,0.1)",
        gridcolor="rgba(127,127,127,0.1)",
        fixedrange=True,
    )
    fig.update_xaxes(
        rangemode="tozero",
        showline=True,
        linecolor="rgba(127,127,127,0.3)",
        gridcolor="rgba(127,127,127,0.3)",
        fixedrange=True,
        showgrid=False,
    )
    fig.update_layout(
        title={
            "text": simple_chart.title,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
            "font": {"size": 14},
        },
        autosize=True,
        bargap=0.30,
        bargroupgap=0.15,
        margin=dict(t=title_height, l=1, r=1, b=1, pad=0),
        uniformtext_minsize=7,
        height=600,
        hoverlabel={"font": {"size": 12}},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode=simple_chart.hover_mode,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="left"),
        showlegend=metric._group_by is not None,
    )
    return fig


def set_heatmap_figure_style(fig, simple_chart: C.SimpleChart):
    fig.update_traces(xgap=1, ygap=1, hovertemplate="%{hovertext} <extra></extra>")
    fig.update_xaxes(
        side="top",
        title=simple_chart.title,
        showgrid=False,
    )
    fig.update_yaxes(
        side="left",
        title="Initial date",
        showgrid=False,
    )
    fig["layout"]["yaxis"]["autorange"] = "reversed"

    if simple_chart.title is not None:
        title_height = len(simple_chart.title.split("<br />")) * 30
    else:
        title_height = 0

    fig.update_layout(
        showlegend=True,
        uniformtext_minsize=9,
        uniformtext_mode="hide",
        autosize=True,
        hoverlabel={"font": {"size": 12}},
        annotations={"font": {"size": 8}},
        hovermode="closest",
        margin=dict(t=title_height, l=1, r=1, b=1, pad=0),
    )
    return fig


def pdf_to_heatmap(
    pdf: pd.DataFrame, chart: C.SimpleChart, metric: M.Metric
) -> Dict[str, List]:

    df = pd.pivot_table(
        pdf,
        values=[C.TEXT_COL, C.Y_AXIS_COL, C.TOOLTIP_COL],
        index=[C.COLOR_COL],
        columns=[C.X_AXIS_COL],
        aggfunc="first",
    )

    x_vals = [*dict.fromkeys([val[1] for val in df.columns.tolist()])]
    if chart.x_axis_tick_labels_func is not None:
        x_vals = [chart.x_axis_tick_labels_func(v, metric) for v in x_vals]

    y_vals = df.index.tolist()
    if chart.y_axis_tick_labels_func is not None:
        y_vals = [chart.y_axis_tick_labels_func(v, metric) for v in y_vals]

    return {
        "x": x_vals,
        "y": [f"{v}." for v in y_vals],
        "z": df[C.Y_AXIS_COL].values.tolist(),
        "hovertext": df[C.TOOLTIP_COL].values.tolist(),
        "annotation_text": df[C.TEXT_COL].values.tolist(),
    }


def plot_chart(
    simple_chart: C.SimpleChart,
    metric: M.Metric,
):
    size = simple_chart.dataframe.shape[0]
    if size > 3000:
        raise Exception(
            f"Too many data points to visualize ({size}), try reducing the scope."
        )

    ct = simple_chart.chart_type
    px.defaults.color_discrete_sequence = PRISM2
    if ct in [
        C.SimpleChartType.BAR,
        C.SimpleChartType.STACKED_BAR,
        C.SimpleChartType.HORIZONTAL_BAR,
        C.SimpleChartType.HORIZONTAL_STACKED_BAR,
    ]:

        barmode = (
            "group"
            if ct in [C.SimpleChartType.BAR, C.SimpleChartType.HORIZONTAL_BAR]
            else "relative"
        )
        orientation = (
            "v" if ct in [C.SimpleChartType.STACKED_BAR, C.SimpleChartType.BAR] else "h"
        )
        fig = px.bar(
            simple_chart.dataframe,
            x=C.X_AXIS_COL,
            y=C.Y_AXIS_COL,
            text=C.TEXT_COL,
            color=C.COLOR_COL,
            barmode=barmode,
            orientation=orientation,
            custom_data=[C.TOOLTIP_COL],
            labels={
                C.X_AXIS_COL: simple_chart.x_axis_label,
                C.Y_AXIS_COL: simple_chart.y_axis_label,
                C.COLOR_COL: simple_chart.color_label,
            },
        )
    elif ct == C.SimpleChartType.LINE:

        fig = px.line(
            simple_chart.dataframe,
            x=C.X_AXIS_COL,
            y=C.Y_AXIS_COL,
            text=C.TEXT_COL,
            color=C.COLOR_COL,
            custom_data=[C.TOOLTIP_COL],
            labels={
                C.X_AXIS_COL: simple_chart.x_axis_label,
                C.Y_AXIS_COL: simple_chart.y_axis_label,
                C.COLOR_COL: simple_chart.color_label,
            },
        )
    elif ct == C.SimpleChartType.STACKED_AREA:
        fig = px.area(
            simple_chart.dataframe,
            x=C.X_AXIS_COL,
            y=C.Y_AXIS_COL,
            text=C.TEXT_COL,
            color=C.COLOR_COL,
            custom_data=[C.TOOLTIP_COL],
            labels={
                C.X_AXIS_COL: simple_chart.x_axis_label,
                C.Y_AXIS_COL: simple_chart.y_axis_label,
                C.COLOR_COL: simple_chart.color_label,
            },
        )
    elif ct == C.SimpleChartType.HEATMAP:
        args = pdf_to_heatmap(
            pdf=simple_chart.dataframe, chart=simple_chart, metric=metric
        )
        fig = ff.create_annotated_heatmap(
            colorscale="BuPu",
            **args,
        )
        fig = set_heatmap_figure_style(fig, simple_chart)
        return fig

    fig.update_traces(hovertemplate="%{customdata[0]} <extra></extra>")
    fig.update_layout(yaxis_ticksuffix=simple_chart.yaxis_ticksuffix)
    fig = set_figure_style(fig, simple_chart, metric)

    return fig
