from __future__ import annotations

import mitzu.model as M
import mitzu.adapters.generic_adapter as GA
from typing import Tuple
import pandas as pd
import mitzu.visualization.common as C

STEP_COL = "_step"


def get_conversion_cols(metric: M.ConversionMetric):
    funnel_length = len(metric._conversion._segments)
    return [f"{GA.AGG_VALUE_COL}_{index}" for index in range(1, funnel_length + 1)]


def fix_conv_times_pdf(
    pdf: pd.DataFrame, metric: M.ConversionMetric
) -> Tuple[pd.DataFrame, int]:
    cols = get_conversion_cols(metric)
    max_ttc = pdf[cols].max(axis=1).max(axis=0)

    for key in cols:
        if 0 <= max_ttc <= C.TTC_RANGE_1_SEC:
            pdf[key] = pdf[key].round(1)
        elif C.TTC_RANGE_1_SEC < max_ttc <= C.TTC_RANGE_2_SEC:
            pdf[key] = pdf[key].div(60).round(1)
        elif C.TTC_RANGE_2_SEC < max_ttc <= C.TTC_RANGE_3_SEC:
            pdf[key] = pdf[key].div(3600).round(1)
        else:
            pdf[key] = pdf[key].div(86400).round(1)
    return pdf


def fix_conversion_na_cols(
    pdf: pd.DataFrame, metric: M.ConversionMetric
) -> pd.DataFrame:
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].fillna("n/a")
    funnel_length = len(metric._conversion._segments)
    cols = [f"{GA.AGG_VALUE_COL}_{i}" for i in range(1, funnel_length + 1)]
    pdf[cols] = pdf[cols].fillna(0)
    return pdf


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
            f"{column_prefix}{i+1}": f"<b>Step {i+1}.</b>"
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


def get_conversion_value_suffix(pdf: pd.DataFrame, metric: M.ConversionMetric) -> str:
    if metric._agg_type in [
        M.AggType.AVERAGE_TIME_TO_CONV,
        M.AggType.PERCENTILE_TIME_TO_CONV,
    ]:
        cols = get_conversion_cols(metric)
        max_ttc = pdf[cols].max(axis=1).max(axis=0)
        return (
            "secs"
            if max_ttc <= C.TTC_RANGE_1_SEC
            else "mins"
            if max_ttc <= C.TTC_RANGE_2_SEC
            else "hours"
            if max_ttc <= C.TTC_RANGE_3_SEC
            else "days"
        )
    else:
        return "%"


def get_conversion_mapping(pdf: pd.DataFrame, metric: M.ConversionMetric):
    mapping = {
        GA.AGG_VALUE_COL: C.Y_AXIS_COL,
        GA.GROUP_COL: C.COLOR_COL,
    }
    if metric._time_group == M.TimeGroup.TOTAL:
        mapping[STEP_COL] = C.X_AXIS_COL
    else:
        mapping[GA.DATETIME_COL] = C.X_AXIS_COL
    return pdf.rename(columns=mapping)
