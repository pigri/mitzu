from __future__ import annotations

from typing import List, Tuple

import mitzu.adapters.generic_adapter as GA
import mitzu.common.model as M
import mitzu.notebook.titles as T
import pandas as pd
import plotly.express as px

MAX_TITLE_LENGTH = 80
STEP_COL = "step"


def fix_na_cols(pdf: pd.DataFrame) -> pd.DataFrame:
    return pdf.apply(lambda x: x.fillna(0) if x.dtype.kind in "biufc" else x.fillna(""))


def filter_top_segmentation_groups(
    pdf: pd.DataFrame,
    metric: M.Metric,
    order_by_col: str = GA.USER_COUNT_COL,
) -> Tuple[List[str], int]:
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
) -> Tuple[List[str], int]:
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
        id_vars=[GA.GROUP_COL, GA.CVR_COL],
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
    pdf1 = get_melted_conv_column(
        f"{GA.USER_COUNT_COL}_", GA.USER_COUNT_COL, pdf, metric
    )
    pdf3 = get_melted_conv_column(
        f"{GA.EVENT_COUNT_COL}_", GA.EVENT_COUNT_COL, pdf, metric
    )

    res = pdf1
    res = pd.merge(
        res,
        pdf3,
        left_on=[GA.GROUP_COL, GA.CVR_COL, STEP_COL],
        right_on=[GA.GROUP_COL, GA.CVR_COL, STEP_COL],
    )
    return res


def get_conversion_hover_template(metric: M.ConversionMetric) -> str:
    tooltip = []
    if metric._time_group == M.TimeGroup.TOTAL:
        if metric._group_by is not None:
            tooltip.append("<b>Group:</b> %{customdata[3]}")
        tooltip.extend(
            [
                "<b>Total Conversion:</b> %{customdata[0]}%",
                "<b>User count:</b> %{customdata[1]}",
                "<b>Event count:</b> %{customdata[2]}",
            ]
        )
    else:
        funnel_length = len(metric._conversion._segments)
        tooltip.append("<b>%{x}</b>")
        if metric._group_by is not None:
            tooltip.append("<b>Group:</b> %{customdata[4]}")

        tooltip.extend(
            [
                "<b>Conversion:</b> %{y}%",
                "<b>User count:</b>",
                " <b>Step 1:</b> %{customdata[0]}",
                f" <b>Step {funnel_length}:</b> %{{customdata[1]}}",
                "<b>Event count:</b>",
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
            "<b>User count:</b> %{y}",
            "<b>Event count:</b> %{customdata[0]}",
        ]
    )
    return "<br />".join(tooltip) + "<extra></extra>"


def get_hover_mode(metric: M.Metric, group_count: int):
    if metric._time_group == M.TimeGroup.TOTAL:
        return "closest"
    else:
        if metric._group_by is None:
            return "x"
        else:
            return "closest" if group_count > 2 else "x"


def plot_conversion(metric: M.ConversionMetric):
    pdf = metric.get_df()
    pdf = fix_na_cols(pdf)

    px.defaults.color_discrete_sequence = px.colors.qualitative.Pastel
    pdf, group_count = filter_top_conversion_groups(pdf, metric)

    if metric._time_group == M.TimeGroup.TOTAL:
        pdf = get_melted_conv_pdf(pdf, metric)
        pdf = pdf.sort_values([STEP_COL], ascending=[True])
        pdf[GA.CVR_COL] = round(pdf[GA.CVR_COL], 2)
        fig = px.bar(
            pdf,
            x=STEP_COL,
            y=GA.USER_COUNT_COL,
            text=GA.USER_COUNT_COL,
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
                GA.USER_COUNT_COL: "Unique user count",
                GA.GROUP_COL: "Group",
            },
        )
        fig.update_traces(
            textposition="auto",
            hovertemplate=get_conversion_hover_template(metric),
        )
    else:
        funnel_length = len(metric._conversion._segments)
        pdf[GA.DATETIME_COL] = pd.to_datetime(
            pdf[GA.DATETIME_COL]
        )  # Serverless bug workaround
        pdf = pdf.sort_values(by=[GA.DATETIME_COL])
        pdf[GA.CVR_COL] = round(pdf[GA.CVR_COL], 2)
        if metric._group_by is None:
            pdf[GA.GROUP_COL] = ""

        fig = px.line(
            pdf,
            x=GA.DATETIME_COL,
            y=GA.CVR_COL,
            text=GA.CVR_COL,
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
                GA.GROUP_COL: "Group",
            },
        )
        fig.update_traces(
            textposition="top center",
            textfont_size=9,
            line=dict(width=3.5),
            mode="markers+lines+text",
            hovertemplate=get_conversion_hover_template(metric),
        )
    title = T.get_conversion_title(metric)
    fig.update_yaxes(rangemode="tozero")
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
        hoverlabel={"font": {"size": 12}},
        hovermode=get_hover_mode(metric, group_count),
    )
    return fig


def plot_segmentation(metric: M.SegmentationMetric):
    pdf = metric.get_df()
    pdf = fix_na_cols(pdf)

    px.defaults.color_discrete_sequence = px.colors.qualitative.Pastel

    pdf, group_count = filter_top_segmentation_groups(pdf, metric)

    if metric._time_group == M.TimeGroup.TOTAL:
        x_title = "segmentation"
        x_title_label = (
            metric._group_by._field._name if metric._group_by is not None else ""
        )
        pdf[x_title] = ""
        pdf = pdf.sort_values([GA.USER_COUNT_COL], ascending=[False])
        fig = px.bar(
            pdf,
            x=x_title,
            y=GA.USER_COUNT_COL,
            color=GA.GROUP_COL,
            barmode="group",
            text=GA.USER_COUNT_COL,
            custom_data=[GA.EVENT_COUNT_COL, GA.GROUP_COL],
            labels={
                x_title: x_title_label,
                GA.GROUP_COL: "Groups",
                GA.USER_COUNT_COL: "Unique user count",
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
                    GA.USER_COUNT_COL: "Unique user count",
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
                    GA.GROUP_COL: "Groups",
                    GA.USER_COUNT_COL: "Unique user count",
                },
            )
        fig.update_traces(
            line=dict(width=3.5),
            textfont_size=9,
            mode="markers+lines+text",
            textposition="top center",
        )
    fig.update_traces(
        hovertemplate=get_segmentation_hover_template(metric),
    )
    fig.update_yaxes(rangemode="tozero")
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
        hoverlabel={"font": {"size": 12}},
        hovermode=get_hover_mode(metric, group_count),
    )
    return fig
