from __future__ import annotations

import mitzu.model as M
import mitzu.adapters.generic_adapter as GA
import pandas as pd
import mitzu.visualization.common as C


RETENTION_PERIOD_COL = "retention_period"


def fix_retention(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf[GA.AGG_VALUE_COL] = round(pdf[GA.AGG_VALUE_COL], 2)
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].fillna("n/a")
    return pdf


def get_retention_period_col(pdf: pd.DataFrame, metric: M.RetentionMetric):
    pdf[RETENTION_PERIOD_COL] = (
        pdf[GA.RETENTION_INDEX].astype(str)
        + " to "
        + (
            pdf[GA.RETENTION_INDEX].apply(
                lambda val: val + metric._retention_window.value
            )
        ).astype(str)
        + f" {metric._retention_window.period.name.lower()}"
    )
    return pdf


def get_retention_mapping(pdf: pd.DataFrame, metric: M.RetentionMetric):
    if metric._time_group == M.TimeGroup.TOTAL:
        mapping = {
            RETENTION_PERIOD_COL: C.X_AXIS_COL,
            GA.AGG_VALUE_COL: C.Y_AXIS_COL,
            GA.GROUP_COL: C.COLOR_COL,
        }
    else:
        if metric._group_by is not None:
            raise Exception(
                "Break downs are not supported for retention over time metric"
            )
        mapping = {
            RETENTION_PERIOD_COL: C.X_AXIS_COL,
            GA.AGG_VALUE_COL: C.Y_AXIS_COL,
            GA.DATETIME_COL: C.COLOR_COL,
        }

    return pdf.rename(columns=mapping)
