from __future__ import annotations

import re
from typing import Tuple

import pandas as pd
import plotly.express as px

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M
import mitzu.titles as T

MAX_TITLE_LENGTH = 80
STEP_COL = "step"
TEXT_COL = "_text"

TTC_RANGE_1_SEC = 600
TTC_RANGE_2_SEC = 7200
TTC_RANGE_3_SEC = 48 * 3600


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


def fix_na_cols(pdf: pd.DataFrame, metric: M.Metric) -> pd.DataFrame:
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].fillna("n/a")

    if isinstance(metric, M.ConversionMetric):
        funnel_length = len(metric._conversion._segments)
        cols = [f"{GA.AGG_VALUE_COL}_{i}" for i in range(1, funnel_length + 1)]
        pdf[cols] = pdf[cols].fillna(0)
    elif isinstance(metric, M.SegmentationMetric):
        pdf[GA.AGG_VALUE_COL] = pdf[GA.AGG_VALUE_COL].fillna(0)

    return pdf


def filter_top_groups(
    pdf: pd.DataFrame,
    metric: M.Metric,
    order_by_col: str = GA.AGG_VALUE_COL,
) -> Tuple[pd.DataFrame, int]:
    max = metric._max_group_count
    g_users = (
        pdf[[GA.GROUP_COL, order_by_col]].groupby(GA.GROUP_COL).sum().reset_index()
    )
    if g_users.shape[0] > 0:
        g_users = g_users.sort_values(order_by_col, ascending=False)
    g_users = g_users.head(max)
    top_groups = list(g_users[GA.GROUP_COL].values)
    return pdf[pdf[GA.GROUP_COL].isin(top_groups)], len(top_groups)


def get_title_height(title: str) -> int:
    return len(title.split("<br />")) * 30


def get_melted_conv_column(
    column_prefix: str, display_name: str, pdf: pd.DataFrame, metric: M.ConversionMetric
) -> pd.DataFrame:
    res = pd.melt(
        pdf,
        id_vars=[GA.GROUP_COL],
        value_vars=[
            f"{column_prefix}{i+1}" for i, _ in enumerate(metric._conversion._segments)
        ],
        var_name=STEP_COL,
        value_name=display_name,
    )
    res[STEP_COL] = res[STEP_COL].replace(
        {
            f"{column_prefix}{i+1}": f"{i+1}. {T.fix_title_text(T.get_segment_title_text(val))}"
            for i, val in enumerate(metric._conversion._segments)
        }
    )
    return res


def get_melted_conv_pdf(pdf: pd.DataFrame, metric: M.ConversionMetric) -> pd.DataFrame:
    pdf1 = get_melted_conv_column(f"{GA.AGG_VALUE_COL}_", GA.AGG_VALUE_COL, pdf, metric)
    pdf2 = get_melted_conv_column(
        f"{GA.USER_COUNT_COL}_", GA.USER_COUNT_COL, pdf, metric
    )
    res = pdf1
    res = pd.merge(
        res,
        pdf2,
        left_on=[GA.GROUP_COL, STEP_COL],
        right_on=[GA.GROUP_COL, STEP_COL],
    )

    return res


def get_conversion_hover_template(metric: M.ConversionMetric, value_suffix: str) -> str:
    tooltip = []
    at = metric._agg_type

    if metric._time_group == M.TimeGroup.TOTAL:
        tooltip.append("<b>Step:</b> %{x}")
        if metric._group_by is not None:
            tooltip.append("<b>Group:</b> %{customdata[2]}")
        tooltip.append(
            f"<b>Value:</b> %{{customdata[1]}} {value_suffix}, %{{customdata[3]}} users"
        )
    else:
        funnel_length = len(metric._conversion._segments)
        tooltip.append("<b>%{x}</b>")
        if metric._group_by is not None:
            tooltip.append("<b>Group:</b> %{customdata[0]}")
        if at == M.AggType.CONVERSION:
            tooltip.append("<b>Total Conversion:</b> %{y}")
        tooltip.append("<b>Values:</b>")
        tooltip.extend(
            [
                f" <b>Step {i}:</b> %{{customdata[{i}]}} {value_suffix}, %{{customdata[{i+funnel_length}]}} users"
                for i in range(1, funnel_length + 1)
            ]
        )
    return "<br />".join(tooltip) + "<extra></extra>"


def get_segmentation_hover_template(metric: M.SegmentationMetric) -> str:
    tooltip = []
    if metric._time_group != M.TimeGroup.TOTAL:
        tooltip.append("<b>%{x}</b>")
    if metric._group_by is not None:
        tooltip.append("<b>Group:</b> %{customdata[0]}")

    tooltip.extend(
        [
            "<b>Value:</b> %{y}",
        ]
    )
    return "<br />".join(tooltip) + "<extra></extra>"


def conv_time_suffix(max_ttc: int) -> str:
    return (
        "secs"
        if max_ttc <= TTC_RANGE_1_SEC
        else "mins"
        if max_ttc <= TTC_RANGE_2_SEC
        else "hours"
        if max_ttc <= TTC_RANGE_3_SEC
        else "days"
    )


def fix_conv_times_pdf(
    pdf: pd.DataFrame, metric: M.ConversionMetric
) -> Tuple[pd.DataFrame, int]:
    funnel_length = len(metric._conversion._segments)
    cols = [f"{GA.AGG_VALUE_COL}_{index}" for index in range(1, funnel_length + 1)]
    max_ttc = pdf[cols].max(axis=1).max(axis=0)
    for key in cols:
        if 0 <= max_ttc <= TTC_RANGE_1_SEC:
            pdf[key] = pdf[key].round(1)
        elif TTC_RANGE_1_SEC < max_ttc <= TTC_RANGE_2_SEC:
            pdf[key] = pdf[key].div(60).round(1)
        elif TTC_RANGE_2_SEC < max_ttc <= TTC_RANGE_3_SEC:
            pdf[key] = pdf[key].div(3600).round(1)
        else:
            pdf[key] = pdf[key].div(86400).round(1)
    return pdf, max_ttc


def fix_conversions_pdf(pdf: pd.DataFrame, metric: M.ConversionMetric) -> pd.DataFrame:
    funnel_length = len(metric._conversion._segments)
    for index in range(1, funnel_length + 1):
        pdf[f"{GA.AGG_VALUE_COL}_{index}"] = pdf[f"{GA.AGG_VALUE_COL}_{index}"].round(2)

    return pdf


def get_hover_mode(metric: M.Metric, group_count: int) -> str:
    if metric._time_group == M.TimeGroup.TOTAL:
        if metric._group_by is None:
            return "x"
        else:
            return "closest" if group_count > 4 else "x"
    else:
        if metric._group_by is None:
            return "x"
        else:
            return "closest" if group_count > 4 else "x"


def get_group_label(metric: M.Metric) -> str:
    if metric._group_by is None:
        return "Group"
    else:
        name = metric._group_by._field._name
        name = re.sub("[^a-zA-Z0-9]", " ", name)
        return name.title()


def plot_conversion(metric: M.ConversionMetric, cached_df: pd.DataFrame = None):
    if cached_df is None:
        pdf = metric.get_df()
    else:
        pdf = cached_df

    pdf = fix_na_cols(pdf, metric)
    px.defaults.color_discrete_sequence = PRISM2
    pdf, group_count = filter_top_groups(
        pdf=pdf, metric=metric, order_by_col=f"{GA.USER_COUNT_COL}_1"
    )
    step_count = len(metric._conversion._segments)
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].astype(str)
    if metric._agg_type in (
        M.AggType.PERCENTILE_TIME_TO_CONV,
        M.AggType.AVERAGE_TIME_TO_CONV,
    ):
        pdf, max_ttc = fix_conv_times_pdf(pdf, metric)
        suffix = conv_time_suffix(max_ttc)
        yaxis_ticksuffix = f" {suffix}"
        agg_value_label = f"P{metric._agg_param} Conversion time"
    else:
        pdf = fix_conversions_pdf(pdf, metric)
        agg_value_label = "Conversion %"
        yaxis_ticksuffix = "%"

    hover_template_value_suffix = yaxis_ticksuffix
    if metric._time_group == M.TimeGroup.TOTAL:
        pdf = get_melted_conv_pdf(pdf, metric)
        viz_agg_col = GA.AGG_VALUE_COL
        if metric._agg_type in (
            M.AggType.PERCENTILE_TIME_TO_CONV,
            M.AggType.AVERAGE_TIME_TO_CONV,
        ):
            pdf[TEXT_COL] = pdf[viz_agg_col].apply(
                lambda val: (f"{val:.1f} {conv_time_suffix(max_ttc)}")
            )
        else:
            pdf[TEXT_COL] = pdf[viz_agg_col].apply(lambda val: f"{val:.1f}%")
        pdf = pdf.sort_values([STEP_COL, GA.USER_COUNT_COL], ascending=[True, False])
        fig = px.bar(
            pdf,
            x=STEP_COL,
            y=viz_agg_col,
            text=TEXT_COL,
            color=GA.GROUP_COL,
            barmode="group",
            custom_data=[
                viz_agg_col,
                GA.AGG_VALUE_COL,
                GA.GROUP_COL,
                GA.USER_COUNT_COL,
            ],
            labels={
                STEP_COL: "Steps",
                viz_agg_col: agg_value_label,
                GA.AGG_VALUE_COL: "",
                GA.GROUP_COL: "",  # get_group_label(metric),
            },
        )
    else:
        viz_agg_col = f"{GA.AGG_VALUE_COL}_{step_count}"
        if metric._agg_type in (
            M.AggType.PERCENTILE_TIME_TO_CONV,
            M.AggType.AVERAGE_TIME_TO_CONV,
        ):
            pdf[TEXT_COL] = pdf[viz_agg_col].apply(
                lambda val: (f"{val:.1f} {conv_time_suffix(max_ttc)}")
            )
        else:
            pdf[TEXT_COL] = pdf[viz_agg_col].apply(lambda val: f"{val:.1f}%")
        pdf[GA.DATETIME_COL] = pd.to_datetime(pdf[GA.DATETIME_COL])
        pdf = pdf.sort_values(by=[GA.DATETIME_COL])
        fig = px.line(
            pdf,
            x=GA.DATETIME_COL,
            y=viz_agg_col,
            text=TEXT_COL,
            color=GA.GROUP_COL,
            custom_data=[GA.GROUP_COL]
            + [f"{GA.AGG_VALUE_COL}_{i}" for i in range(1, step_count + 1)]
            + [f"{GA.USER_COUNT_COL}_{i}" for i in range(1, step_count + 1)],
            labels={
                GA.DATETIME_COL: "",
                viz_agg_col: agg_value_label,
                GA.GROUP_COL: "",  # get_group_label(metric),
            },
        )

    fig.update_traces(
        hovertemplate=get_conversion_hover_template(metric, hover_template_value_suffix)
    )
    fig.update_layout(yaxis_ticksuffix=yaxis_ticksuffix)

    title = T.get_conversion_title(metric)
    fig = set_figure_style(fig=fig, title=title, metric=metric, group_count=group_count)
    return fig


def plot_segmentation(metric: M.SegmentationMetric, cached_df: pd.DataFrame = None):
    if cached_df is None:
        pdf = metric.get_df()
    else:
        pdf = cached_df
    pdf = fix_na_cols(pdf, metric)

    px.defaults.color_discrete_sequence = PRISM2
    pdf, group_count = filter_top_groups(pdf, metric)
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].astype(str)

    agg_value_label = (
        "Events" if metric._agg_type == M.AggType.COUNT_EVENTS else "Unique users"
    )

    if metric._time_group == M.TimeGroup.TOTAL:
        x_title = "segmentation"
        x_title_label = (
            metric._group_by._field._name if metric._group_by is not None else ""
        )
        pdf[x_title] = ""
        pdf[TEXT_COL] = pdf[GA.AGG_VALUE_COL]
        pdf = pdf.sort_values([GA.AGG_VALUE_COL], ascending=[False])
        fig = px.bar(
            pdf,
            x=x_title,
            y=GA.AGG_VALUE_COL,
            color=GA.GROUP_COL,
            barmode="group",
            text=TEXT_COL,
            custom_data=[GA.GROUP_COL],
            labels={
                x_title: x_title_label,
                GA.GROUP_COL: "",  # get_group_label(metric),
                GA.AGG_VALUE_COL: agg_value_label,
            },
        )
    else:
        pdf = pdf.sort_values(by=[GA.DATETIME_COL])
        if metric._group_by is None:
            fig = px.line(
                pdf,
                x=GA.DATETIME_COL,
                y=GA.AGG_VALUE_COL,
                text=GA.AGG_VALUE_COL,
                custom_data=[GA.GROUP_COL],
                labels={
                    GA.DATETIME_COL: "",
                    GA.AGG_VALUE_COL: agg_value_label,
                },
            )
        else:
            fig = px.line(
                pdf,
                x=GA.DATETIME_COL,
                y=GA.AGG_VALUE_COL,
                color=GA.GROUP_COL,
                text=GA.AGG_VALUE_COL,
                custom_data=[GA.GROUP_COL],
                labels={
                    GA.DATETIME_COL: "",
                    GA.GROUP_COL: "",
                    GA.AGG_VALUE_COL: agg_value_label,
                },
            )

    title = T.get_segmentation_title(metric)
    fig.update_traces(hovertemplate=get_segmentation_hover_template(metric))
    fig = set_figure_style(fig=fig, title=title, metric=metric, group_count=group_count)
    return fig


def set_figure_style(fig, title: str, metric: M.Metric, group_count: int):
    if metric._config.time_group != M.TimeGroup.TOTAL:
        fig.update_traces(
            line=dict(width=2.5), mode="lines+markers", textposition="top center"
        )
    else:
        fig.update_traces(textposition="outside")

    fig.update_traces(textfont_size=9)
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
            "text": title,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
            "font": {"size": 14},
        },
        autosize=True,
        bargap=0.30,
        bargroupgap=0.15,
        margin=dict(t=get_title_height(title), l=1, r=1, b=1, pad=0),
        uniformtext_minsize=7,
        height=600,
        hoverlabel={"font": {"size": 12}},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode=get_hover_mode(metric, group_count),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="left"),
        showlegend=metric._group_by is not None,
    )
    return fig
