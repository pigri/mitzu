from __future__ import annotations

import mitzu.model as M
import mitzu.adapters.generic_adapter as GA
import pandas as pd
import mitzu.visualization.labels as L
import mitzu.visualization.common as C
import mitzu.visualization.tooltips as T
import mitzu.visualization.titles as TI
import mitzu.visualization.transform_conv as TC
import mitzu.visualization.transform_retention as TR
from mitzu.helper import value_to_label
from typing import Optional

STEP_COL = "_step"


def filter_top_groups(
    pdf: pd.DataFrame,
    metric: M.Metric,
    order_by_col: str,
) -> pd.DataFrame:
    max = metric._max_group_count
    g_users = (
        pdf[[GA.GROUP_COL, order_by_col]].groupby(GA.GROUP_COL).sum().reset_index()
    )
    if g_users.shape[0] > 0:
        g_users = g_users.sort_values(order_by_col, ascending=False)
    g_users = g_users.head(max)
    top_groups = list(g_users[GA.GROUP_COL].values)
    return pdf[pdf[GA.GROUP_COL].isin(top_groups)]


def get_color_label(metric: M.Metric):
    if metric._group_by is not None:
        return value_to_label(metric._group_by._field._get_name())
    elif (
        isinstance(metric, M.RetentionMetric)
        and metric._chart_type == M.SimpleChartType.LINE
    ):
        return "Cohort"
    return ""


def get_hover_mode(pdf: pd.DataFrame, metric: M.Metric) -> str:
    group_count = len(list(pdf[C.COLOR_COL].values))
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


def get_default_chart_type(metric: M.Metric) -> M.SimpleChartType:
    if metric._time_group == M.TimeGroup.TOTAL:
        return M.SimpleChartType.BAR
    else:
        if isinstance(metric, M.SegmentationMetric) or isinstance(
            metric, M.ConversionMetric
        ):
            return M.SimpleChartType.LINE
        elif isinstance(metric, M.RetentionMetric):
            return M.SimpleChartType.HEATMAP
        else:
            raise ValueError(f"No default chart type defined for {type(metric)}")


def get_preprocessed_conversion_dataframe(
    pdf: pd.DataFrame, metric: M.ConversionMetric, suffix: str
) -> pd.DataFrame:
    pdf = TC.fix_conversion_na_cols(pdf, metric)
    if metric._agg_type in [
        M.AggType.AVERAGE_TIME_TO_CONV,
        M.AggType.PERCENTILE_TIME_TO_CONV,
    ]:
        pdf = TC.fix_conv_times_pdf(pdf, metric)

    pdf = filter_top_groups(pdf, metric, order_by_col=f"{GA.USER_COUNT_COL}_1")

    if metric._time_group == M.TimeGroup.TOTAL:
        pdf = TC.get_melted_conv_pdf(pdf, metric)
    else:
        funnel_length = len(metric._conversion._segments)
        pdf[GA.AGG_VALUE_COL] = pdf[f"{GA.AGG_VALUE_COL}_{funnel_length}"]
        pdf[GA.USER_COUNT_COL] = pdf[f"{GA.USER_COUNT_COL}_{funnel_length}"]

    pdf = TC.get_conversion_mapping(pdf, metric)

    pdf[C.TEXT_COL] = pdf[C.Y_AXIS_COL].apply(lambda val: (f"{val:.1f} {suffix}"))
    pdf = T.add_conversion_tooltip(pdf, metric, suffix)

    pdf = pdf.sort_values(
        [C.X_AXIS_COL, C.COLOR_COL, GA.USER_COUNT_COL], ascending=[True, True, False]
    )
    return pdf


def get_preprocessed_segmentation_dataframe(
    pdf: pd.DataFrame, metric: M.SegmentationMetric
):
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].fillna("n/a").astype(str).fillna("n/a")
    pdf = filter_top_groups(pdf, metric, order_by_col=GA.AGG_VALUE_COL)
    pdf = pdf.rename(
        columns={
            GA.DATETIME_COL: C.X_AXIS_COL,
            GA.AGG_VALUE_COL: C.Y_AXIS_COL,
            GA.GROUP_COL: C.COLOR_COL,
        }
    )
    pdf[C.TEXT_COL] = pdf[C.Y_AXIS_COL]
    pdf = pdf.sort_values([C.X_AXIS_COL, C.Y_AXIS_COL], ascending=[True, False])
    pdf = T.get_segmentation_tooltip(pdf, metric)

    pdf[C.COLOR_COL] = pdf[C.COLOR_COL].fillna("")
    pdf[C.X_AXIS_COL] = pdf[C.X_AXIS_COL].fillna("")

    return pdf


def get_preprocessed_retention_dataframe(
    pdf: pd.DataFrame, metric: M.RetentionMetric
) -> pd.DataFrame:
    size = pdf.shape[0]
    pdf = TR.fix_retention(pdf, metric)
    pdf = filter_top_groups(pdf, metric, order_by_col=GA.USER_COUNT_COL + "_1")
    pdf = TR.get_retention_mapping(pdf, metric)
    pdf = T.get_retention_tooltip(pdf, metric)
    pdf[C.TEXT_COL] = pdf[C.Y_AXIS_COL].apply(
        lambda val: f"{val:.1f}%" if val > 0 and size <= 200 else ""
    )
    pdf = pdf.sort_values([C.X_AXIS_COL, C.COLOR_COL], ascending=[True, True])
    return pdf


def get_simple_chart(
    metric: M.Metric, result_df: Optional[pd.DataFrame] = None
) -> C.SimpleChart:
    if metric._chart_type is None:
        chart_type = get_default_chart_type(metric)
    else:
        chart_type = metric._chart_type

    if chart_type == M.SimpleChartType.HEATMAP:
        y_axis_label = get_color_label(metric)
        color_label = L.agg_type_label(metric._agg_type, metric._agg_param)
    else:
        y_axis_label = L.agg_type_label(metric._agg_type, metric._agg_param)
        color_label = get_color_label(metric)

    if (
        chart_type not in (M.SimpleChartType.LINE, M.SimpleChartType.STACKED_AREA)
        and metric._time_group != M.TimeGroup.TOTAL
    ):
        x_axis_label_func = C.fix_date_label
    else:
        x_axis_label_func = None

    if result_df is None:
        result_df = metric.get_df()

    if isinstance(metric, M.SegmentationMetric):
        pdf = get_preprocessed_segmentation_dataframe(result_df, metric)
        return C.SimpleChart(
            x_axis_label="",
            y_axis_label=y_axis_label,
            color_label=color_label,
            hover_mode=get_hover_mode(pdf, metric),
            chart_type=chart_type,
            title=TI.get_segmentation_title(metric),
            yaxis_ticksuffix="",
            dataframe=pdf,
            x_axis_labels_func=x_axis_label_func,
        )

    if isinstance(metric, M.ConversionMetric):
        suffix = TC.get_conversion_value_suffix(result_df, metric)
        pdf = get_preprocessed_conversion_dataframe(result_df, metric, suffix)
        return C.SimpleChart(
            x_axis_label="",
            y_axis_label=y_axis_label,
            color_label=color_label,
            title=TI.get_conversion_title(metric),
            chart_type=chart_type,
            hover_mode=get_hover_mode(pdf, metric),
            yaxis_ticksuffix=suffix,
            dataframe=pdf,
            x_axis_labels_func=x_axis_label_func,
        )

    if isinstance(metric, M.RetentionMetric):
        pdf = get_preprocessed_retention_dataframe(result_df, metric)
        return C.SimpleChart(
            x_axis_label="",
            y_axis_label=y_axis_label,
            color_label=color_label,
            title=TI.get_retention_title(metric),
            chart_type=chart_type,
            hover_mode="closest",
            yaxis_ticksuffix="%",
            dataframe=pdf,
            x_axis_labels_func=C.retention_period_label,
        )

    raise Exception(f"Unsupported metric type for visualization {type(metric)}")
