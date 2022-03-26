from __future__ import annotations
import pandas as pd  # type: ignore
import plotly.express as px  # type: ignore
from typing import List, cast
import mitzu.common.model as M
import mitzu.adapters.generic_adapter as GA

MAX_TITLE_LENGTH = 80
STEP_COL = "step"


def fix_na_cols(pdf: pd.DataFrame) -> pd.DataFrame:
    return pdf.apply(lambda x: x.fillna(0) if x.dtype.kind in "biufc" else x.fillna(""))


def filter_top_segmentation_groups(
    pdf: pd.DataFrame,
    metric: M.Metric,
    order_by_col: str = GA.USER_COUNT_COL,
) -> List[str]:
    max = metric._max_group_count
    g_users = (
        pdf[[GA.GROUP_COL, order_by_col]].groupby(GA.GROUP_COL).sum().reset_index()
    )
    if g_users.shape[0] > 0:
        g_users = g_users.sort_values(order_by_col, ascending=False)
    g_users = g_users.head(max)
    top_groups = list(g_users[GA.GROUP_COL].values)
    return pdf[pdf[GA.GROUP_COL].isin(top_groups)]


def filter_top_conversion_groups(
    pdf: pd.DataFrame, metric: M.ConversionMetric
) -> List[str]:
    return filter_top_segmentation_groups(
        pdf=pdf, metric=metric, order_by_col=f"{GA.USER_COUNT_COL}_1"
    )


def get_segmented_by_str(metric: M.Metric) -> str:
    if metric._group_by:
        grp = metric._group_by._field._name
        return f"segmented by <b>{grp}</b> (top {metric._max_group_count})"
    return ""


def fix_title_text(title_text: str, max_length=MAX_TITLE_LENGTH) -> str:
    if len(title_text) > max_length:
        return title_text[:max_length] + "..."
    else:
        return title_text


def get_segment_title_text(segment: M.Segment) -> str:
    if isinstance(segment, M.SimpleSegment):
        s = cast(M.SimpleSegment, segment)
        if s._operator is None:
            return s._left._event_name
        else:
            left = cast(M.EventFieldDef, s._left)
            right = s._right
            if right is None:
                right = "null"

            return f"{left._event_name} with {left._field._name} {s._operator} {right}"
    elif isinstance(segment, M.ComplexSegment):
        c = cast(M.ComplexSegment, segment)
        return f"{get_segment_title_text(c._left)} {c._operator} {get_segment_title_text(c._right)}"
    else:
        raise ValueError(f"Segment of type {type(segment)} is not supported.")


def get_time_group_text(time_group: M.TimeGroup) -> str:
    if time_group == M.TimeGroup.TOTAL:
        return "total"
    if time_group == M.TimeGroup.DAY:
        return "daily"
    if time_group == M.TimeGroup.MINUTE:
        return "minute by minute"

    return time_group.name.lower() + "ly"


def get_segmentation_title(metric: M.SegmentationMetric):
    if metric._custom_title:
        return metric._custom_title
    title_text = fix_title_text(get_segment_title_text(metric._segment))
    event = f"<b>{title_text}</b>"
    time_group = metric._time_group
    tg = get_time_group_text(time_group).title()
    agg = "count of unique users"
    segment_by = get_segmented_by_str(metric)
    return f"{tg} {agg}<br />who did {event}<br />{segment_by}"


def get_conversion_title(metric: M.ConversionMetric) -> str:
    if metric._custom_title:
        return metric._custom_title
    events = " then did ".join(
        [
            f"<b>{fix_title_text(get_segment_title_text(seg), 100)}</b>"
            for seg in metric._conversion._segments
        ]
    )
    time_group = metric._time_group
    agg = "count and cvr. of unique users"
    tg = get_time_group_text(time_group).title()

    if time_group != M.TimeGroup.TOTAL:
        agg = "conversion rate of unique users"

    within_str = f"within {metric._conv_window}"
    segment_by = get_segmented_by_str(metric)
    return f"{tg} {agg}<br />who did {events}<br />{within_str} {segment_by}"


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
            f"{column_prefix}{i+1}": f"{i+1}. {fix_title_text(get_segment_title_text(val))}"
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


def plot_conversion(metric: M.ConversionMetric):
    pdf = metric.get_df()
    pdf = fix_na_cols(pdf)

    px.defaults.color_discrete_sequence = px.colors.qualitative.Pastel
    pdf = filter_top_conversion_groups(pdf, metric)

    if metric._time_group == M.TimeGroup.TOTAL:
        pdf = get_melted_conv_pdf(pdf, metric)
        pdf = pdf.sort_values([STEP_COL], ascending=[True])
        pdf[GA.CVR_COL] = round(pdf[GA.CVR_COL] * 100, 2)
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
                STEP_COL: get_conversion_title(metric),
                GA.CVR_COL: "Conversion",
                GA.USER_COUNT_COL: "Unique user count",
                GA.GROUP_COL: "Group",
            },
        )
        fig.update_traces(
            textposition="auto",
            hovertemplate="""<b>%{x}</b>
                    <br /><b>%{customdata[3]}</b>
                    <br />
                    <br />Total Conversion: <b>%{customdata[0]}%</b>
                    <br />User count: <b>%{customdata[1]}</b>
                    <br />Event count: <b>%{customdata[2]}</b>                                       
                     <extra></extra>
            """,
        )
    else:
        funnel_length = len(metric._conversion._segments)
        pdf[GA.DATETIME_COL] = pd.to_datetime(
            pdf[GA.DATETIME_COL]
        )  # Serverless bug workaround
        pdf = pdf.sort_values(by=[GA.DATETIME_COL])
        pdf[GA.CVR_COL] = round(pdf[GA.CVR_COL] * 100, 2)
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
                GA.DATETIME_COL: get_conversion_title(metric),
                GA.CVR_COL: "Conversion",
            },
        )
        fig.update_traces(
            textposition="top center",
            textfont_size=9,
            line=dict(width=3.5),
            mode="markers+lines+text",
            hovertemplate="""<b>%{x}</b>
                    <br /><b>%{customdata[4]}</b>
                    <br />
                    <br />Conversion: <b>%{y}%</b>             
                    <br />Activated users:<b>%{customdata[0]}</b>                    
                    <br />Converted users: <b>%{customdata[1]}</b>
                    <br />
                    <br />Activated event count:<b>%{customdata[2]}</b>                    
                    <br />Converted event count:<b>%{customdata[3]}</b>                            
                    <extra></extra>
            """,
        )
    fig.update_yaxes(rangemode="tozero")
    fig.update_layout(
        autosize=True,
        bargap=0.30,
        bargroupgap=0.15,
        margin=dict(t=5, l=5, r=5, b=15, pad=1),
        uniformtext_minsize=8,
        uniformtext_mode="hide",
        hoverlabel={"font": {"size": 12}},
        hovermode="closest",
    )
    return fig


def plot_segmentation(metric: M.SegmentationMetric):
    pdf = metric.get_df()
    pdf = fix_na_cols(pdf)

    px.defaults.color_discrete_sequence = px.colors.qualitative.Pastel

    pdf = filter_top_segmentation_groups(pdf, metric)

    if metric._time_group == M.TimeGroup.TOTAL:
        pdf["segmentation"] = ""
        pdf = pdf.sort_values([GA.USER_COUNT_COL], ascending=[False])
        fig = px.bar(
            pdf,
            x="segmentation",
            y=GA.USER_COUNT_COL,
            color=GA.GROUP_COL,
            barmode="group",
            text=GA.USER_COUNT_COL,
            custom_data=[GA.EVENT_COUNT_COL, GA.GROUP_COL],
            labels={
                "segmentation": get_segmentation_title(metric),
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
                    GA.DATETIME_COL: get_segmentation_title(metric),
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
                    GA.DATETIME_COL: get_segmentation_title(metric),
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
        hovertemplate="""<b>%{x}</b> 
            <br /><b>%{customdata[1]}</b>
            <br />
            <br />User count: <b>%{y}</b>
            <br />Event count: <b>%{customdata[0]}</b>
             <extra></extra>
        """,
    )
    fig.update_yaxes(rangemode="tozero")
    fig.update_layout(
        bargap=0.30,
        bargroupgap=0.15,
        uniformtext_minsize=9,
        uniformtext_mode="hide",
        autosize=True,
        margin=dict(t=5, l=5, r=5, b=15, pad=1),
        hoverlabel={"font": {"size": 12}},
        hovermode="closest",
    )
    return fig
