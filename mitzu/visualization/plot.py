from __future__ import annotations


import plotly.express as px


import mitzu.model as M
import mitzu.visualization.common as C


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


def plot_chart(
    simple_chart: C.SimpleChart,
    metric: M.Metric,
):
    px.defaults.color_discrete_sequence = PRISM2
    if simple_chart.chart_type == C.SimpleChartType.BAR:
        fig = px.bar(
            simple_chart.dataframe,
            x=C.X_AXIS_COL,
            y=C.Y_AXIS_COL,
            text=C.TEXT_COL,
            color=C.COLOR_COL,
            barmode="group",
            custom_data=[C.TOOLTIP_COL],
            labels={
                C.X_AXIS_COL: simple_chart.x_axis_label,
                C.Y_AXIS_COL: simple_chart.y_axis_label,
                C.COLOR_COL: simple_chart.color_label,
            },
        )
    elif simple_chart.chart_type == C.SimpleChartType.LINE:
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

    fig.update_traces(hovertemplate="%{customdata[0]} <extra></extra>")
    fig.update_layout(yaxis_ticksuffix=simple_chart.yaxis_ticksuffix)
    fig = set_figure_style(fig, simple_chart, metric)

    return fig
