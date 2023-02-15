from __future__ import annotations

import mitzu.model as M
import pandas as pd
import mitzu.visualization.common as C
import mitzu.adapters.generic_adapter as GA
import mitzu.visualization.labels as L


def add_conversion_tooltip(
    pdf: pd.DataFrame, metric: M.ConversionMetric, value_suffix: str
) -> str:
    agg_type_str = L.agg_type_label(metric._agg_type, metric._agg_param)

    if metric._time_group == M.TimeGroup.TOTAL:
        pdf[C.TOOLTIP_COL] = (
            "<b>Step: </b>"
            + pdf[C.X_AXIS_COL].astype(str)
            + "<br /><b>Group: </b>"
            + pdf[C.COLOR_COL].astype(str)
            + f"<br /><br /><b>{agg_type_str}: </b>"
            + round(pdf[C.Y_AXIS_COL], 2).astype(str)
            + f" {value_suffix}"
            + "<br /><b>User count: </b>"
            + pdf[GA.USER_COUNT_COL].astype(str)
        )
    else:
        tooltip = (
            "<b>Datetime: </b>"
            + pdf[C.X_AXIS_COL].astype(str)
            + "<br /><b>Group: </b>"
            + pdf[C.COLOR_COL].astype(str)
        )
        funnel_length = len(metric._conversion._segments)
        for step in range(1, funnel_length + 1):
            tooltip += (
                f"<br /><br /><b>Step: {step}.</b>"
                + f"<br /><b>{agg_type_str}: </b>"
                + round(pdf[GA.AGG_VALUE_COL + f"_{step}"], 2).astype(str)
                + f" {value_suffix}"
                + "<br /><b>User count: </b>"
                + pdf[GA.USER_COUNT_COL + f"_{step}"].astype(str)
            )

        pdf[C.TOOLTIP_COL] = tooltip

    return pdf


def get_segmentation_tooltip(
    pdf: pd.DataFrame,
    metric: M.SegmentationMetric,
) -> pd.DataFrame:
    agg_type_str = L.agg_type_label(metric._agg_type, metric._agg_param)
    if metric._time_group == M.TimeGroup.TOTAL:
        pdf[C.TOOLTIP_COL] = (
            "<br /><b>Group: </b>"
            + pdf[C.COLOR_COL].astype(str)
            + f"<br /><br /><b>{agg_type_str}: </b>"
            + round(pdf[C.Y_AXIS_COL], 2).astype(str)
        )
    else:
        pdf[C.TOOLTIP_COL] = (
            "<b>Datetime: </b>"
            + pdf[C.X_AXIS_COL].astype(str)
            + "<br /><b>Group: </b>"
            + pdf[C.COLOR_COL].astype(str)
            + f"<br /><br /><b>{agg_type_str}: </b>"
            + round(pdf[C.Y_AXIS_COL], 2).astype(str)
        )
    return pdf


def get_retention_tooltip(pdf: pd.DataFrame, metric: M.RetentionMetric) -> pd.DataFrame:
    agg_type_str = L.agg_type_label(metric._agg_type, metric._agg_param)
    value_suffix = "%"
    pdf[C.TOOLTIP_COL] = (
        "<b>Retention period: </b>"
        + (
            pdf[C.X_AXIS_COL]
            .apply(lambda val: C.retention_period_label(val, metric))
            .astype(str)
        )
        + "<br /><b>Cohort: </b>"
        + pdf[C.COLOR_COL].astype(str)
        + f"<br /><br /><b>{agg_type_str}: </b>"
        + round(pdf[C.Y_AXIS_COL], 2).astype(str)
        + f" {value_suffix}"
        + "<br /><b>Initial user count: </b>"
        + pdf[GA.USER_COUNT_COL + "_1"].astype(str)
        + "<br /><b>Retaining user count: </b>"
        + pdf[GA.USER_COUNT_COL + "_2"].astype(str)
    )
    return pdf
