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


def fix_na_cols(pdf: pd.DataFrame, metric: M.Metric) -> pd.DataFrame:
    if metric._group_by is not None:
        pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].fillna("n/a")
    pdf = pdf.apply(lambda x: x.fillna(0) if x.dtype.kind in "biufc" else x.fillna(""))
    return pdf


def filter_top_segmentation_groups(
    pdf: pd.DataFrame,
    metric: M.Metric,
    order_by_col: str = GA.USER_COUNT_COL,
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


def filter_top_conversion_groups(
    pdf: pd.DataFrame, metric: M.ConversionMetric
) -> Tuple[pd.DataFrame, int]:
    return filter_top_segmentation_groups(
        pdf=pdf, metric=metric, order_by_col=f"{GA.USER_COUNT_COL}_1"
    )


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
    for index, seg in enumerate(metric._conversion._segments):
        i = index + 1
        pdf[f"{GA.CVR_COL}_{i}"] = round(
            pdf[f"{GA.USER_COUNT_COL}_{i}"] * 100 / pdf[f"{GA.USER_COUNT_COL}_1"], 2
        )
    pdf1 = get_melted_conv_column(
        f"{GA.USER_COUNT_COL}_", GA.USER_COUNT_COL, pdf, metric
    )
    pdf2 = get_melted_conv_column(f"{GA.CVR_COL}_", GA.CVR_COL, pdf, metric)
    pdf3 = get_melted_conv_column(
        f"{GA.EVENT_COUNT_COL}_", GA.EVENT_COUNT_COL, pdf, metric
    )
    res = pdf1
    res = pd.merge(
        res,
        pdf2,
        left_on=[GA.GROUP_COL, STEP_COL],
        right_on=[GA.GROUP_COL, STEP_COL],
    )
    res = pd.merge(
        res,
        pdf3,
        left_on=[GA.GROUP_COL, STEP_COL],
        right_on=[GA.GROUP_COL, STEP_COL],
    )
    return res


def get_conversion_hover_template(metric: M.ConversionMetric) -> str:
    tooltip = []
    if metric._time_group == M.TimeGroup.TOTAL:
        tooltip.append("<b>Step:</b> %{x}")
        if metric._group_by is not None:
            tooltip.append("<b>Group:</b> %{customdata[3]}")
        tooltip.extend(
            [
                "<b>Conversion:</b> %{customdata[0]}%",
                "<b>User Count:</b> %{customdata[1]}",
                "<b>Event Count:</b> %{customdata[2]}",
            ]
        )
    else:
        funnel_length = len(metric._conversion._segments)
        tooltip.append("<b>%{x}</b>")
        if metric._group_by is not None:
            tooltip.append("<b>Group:</b> %{customdata[4]}")

        tooltip.extend(
            [
                "<b>Conversion:</b> %{y}",
                "<b>User Count:</b>",
                " <b>Step 1:</b> %{customdata[0]}",
                f" <b>Step {funnel_length}:</b> %{{customdata[1]}}",
                "<b>Event Count:</b>",
                " <b>Step 1:</b>%{customdata[2]}",
                f" <b>Step {funnel_length}:</b> %{{customdata[3]}}",
            ]
        )
    return "<br />".join(tooltip) + "<extra></extra>"


def get_segmentation_hover_template(metric: M.SegmentationMetric) -> str:
    tooltip = []
    if metric._time_group != M.TimeGroup.TOTAL:
        tooltip.append("<b>%{x}</b>")
    if metric._group_by is not None:
        tooltip.append("<b>Group:</b> %{customdata[1]}")

    tooltip.extend(
        [
            "<b>User Count:</b> %{y}",
            "<b>Event Count:</b> %{customdata[0]}",
        ]
    )
    return "<br />".join(tooltip) + "<extra></extra>"


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
            return "closest" if group_count > 2 else "x"


def get_group_label(metric: M.Metric) -> str:
    if metric._group_by is None:
        return "Group"
    else:
        name = metric._group_by._field._name
        name = re.sub("[^a-zA-Z0-9]", " ", name)
        return name.title()


def plot_conversion(metric: M.ConversionMetric):
    pdf = metric.get_df()
    pdf = fix_na_cols(pdf, metric)
    px.defaults.color_discrete_sequence = px.colors.qualitative.Safe
    pdf, group_count = filter_top_conversion_groups(pdf, metric)
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].astype(str)
    pdf[GA.CVR_COL] = round(pdf[GA.CVR_COL], 2)
    if metric._time_group == M.TimeGroup.TOTAL:
        pdf = get_melted_conv_pdf(pdf, metric)
        pdf = pdf.sort_values([STEP_COL], ascending=[True])
        pdf[TEXT_COL] = (
            pdf[GA.CVR_COL].apply(lambda val: f"{val:.1f}%")
            + "<br>"
            + pdf[GA.GROUP_COL]
        )

        fig = px.bar(
            pdf,
            x=STEP_COL,
            y=GA.CVR_COL,
            text=TEXT_COL,
            color=GA.GROUP_COL,
            barmode="group",
            custom_data=[
                GA.CVR_COL,
                GA.USER_COUNT_COL,
                GA.EVENT_COUNT_COL,
                GA.GROUP_COL,
            ],
            labels={
                STEP_COL: "Steps",
                GA.CVR_COL: "Conversion",
                GA.USER_COUNT_COL: "Unique User Count",
                GA.GROUP_COL: "",  # get_group_label(metric),
            },
        )
        fig.update_traces(
            textposition="auto",
            hovertemplate=get_conversion_hover_template(metric),
        )
    else:
        funnel_length = len(metric._conversion._segments)
        pdf[GA.DATETIME_COL] = pd.to_datetime(pdf[GA.DATETIME_COL])
        pdf = pdf.sort_values(by=[GA.DATETIME_COL])
        pdf[TEXT_COL] = pdf[GA.CVR_COL].apply(lambda val: f"{val:.1f}%")
        fig = px.line(
            pdf,
            x=GA.DATETIME_COL,
            y=GA.CVR_COL,
            text=TEXT_COL,
            color=GA.GROUP_COL,
            custom_data=[
                f"{GA.USER_COUNT_COL}_1",
                f"{GA.USER_COUNT_COL}_{funnel_length}",
                f"{GA.EVENT_COUNT_COL}_1",
                f"{GA.EVENT_COUNT_COL}_{funnel_length}",
                GA.GROUP_COL,
            ],
            labels={
                GA.DATETIME_COL: "",
                GA.CVR_COL: "Conversion",
                GA.GROUP_COL: "",  # get_group_label(metric),
            },
        )
        fig.update_traces(
            textposition="top center",
            textfont_size=9,
            line=dict(width=3.5),
            mode="lines",
            hovertemplate=get_conversion_hover_template(metric),
        )

    fig.update_layout(yaxis_ticksuffix="%")
    if metric._config.custom_title is not None:
        title = metric._config.custom_title
    else:
        title = T.get_conversion_title(metric)

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
        uniformtext_minsize=8,
        uniformtext_mode="hide",
        height=600,
        hoverlabel={"font": {"size": 12}},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode=get_hover_mode(metric, group_count),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="left"),
    )
    return fig


def plot_segmentation(metric: M.SegmentationMetric):
    pdf = metric.get_df()
    pdf = fix_na_cols(pdf, metric)

    px.defaults.color_discrete_sequence = px.colors.qualitative.Safe

    pdf, group_count = filter_top_segmentation_groups(pdf, metric)
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].astype(str)

    if metric._time_group == M.TimeGroup.TOTAL:
        x_title = "segmentation"
        x_title_label = (
            metric._group_by._field._name if metric._group_by is not None else ""
        )
        pdf[x_title] = ""
        pdf[TEXT_COL] = pdf[GA.USER_COUNT_COL].astype(str) + "<br>" + pdf[GA.GROUP_COL]
        pdf = pdf.sort_values([GA.USER_COUNT_COL], ascending=[False])
        fig = px.bar(
            pdf,
            x=x_title,
            y=GA.USER_COUNT_COL,
            color=GA.GROUP_COL,
            barmode="group",
            text=TEXT_COL,
            custom_data=[GA.EVENT_COUNT_COL, GA.GROUP_COL],
            labels={
                x_title: x_title_label,
                GA.GROUP_COL: "",  # get_group_label(metric),
                GA.USER_COUNT_COL: "Unique User Count",
            },
        )
        fig.update_traces(textposition="auto")
    else:
        pdf = pdf.sort_values(by=[GA.DATETIME_COL])
        if metric._group_by is None:
            fig = px.line(
                pdf,
                x=GA.DATETIME_COL,
                y=GA.USER_COUNT_COL,
                text=GA.USER_COUNT_COL,
                custom_data=[GA.EVENT_COUNT_COL, GA.GROUP_COL],
                labels={
                    GA.DATETIME_COL: "",
                    GA.USER_COUNT_COL: "Unique User Count",
                },
            )
        else:
            fig = px.line(
                pdf,
                x=GA.DATETIME_COL,
                y=GA.USER_COUNT_COL,
                color=GA.GROUP_COL,
                text=GA.USER_COUNT_COL,
                custom_data=[GA.EVENT_COUNT_COL, GA.GROUP_COL],
                labels={
                    GA.DATETIME_COL: "",
                    GA.GROUP_COL: "",  # get_group_label(metric),
                    GA.USER_COUNT_COL: "Unique User Count",
                },
            )
        fig.update_traces(
            line=dict(width=3.5),
            textfont_size=9,
            mode="lines",
            textposition="top center",
        )
    fig.update_traces(
        hovertemplate=get_segmentation_hover_template(metric),
    )
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
    if metric._config.custom_title is not None:
        title = metric._config.custom_title
    else:
        title = T.get_segmentation_title(metric)

    fig.update_layout(
        title={
            "text": title,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
            "font": {"size": 14},
        },
        bargap=0.30,
        bargroupgap=0.15,
        uniformtext_minsize=9,
        uniformtext_mode="hide",
        autosize=True,
        margin=dict(t=get_title_height(title), l=1, r=1, b=1, pad=0),
        height=600,
        hoverlabel={"font": {"size": 12}},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode=get_hover_mode(metric, group_count),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="left"),
    )

    return fig
