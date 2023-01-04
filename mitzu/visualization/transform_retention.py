from __future__ import annotations

import mitzu.model as M
import mitzu.adapters.generic_adapter as GA
import pandas as pd
import mitzu.visualization.common as C


def fix_retention(pdf: pd.DataFrame, metric: M.RetentionMetric) -> pd.DataFrame:
    pdf[GA.AGG_VALUE_COL] = round(pdf[GA.AGG_VALUE_COL], 2)
    pdf[GA.GROUP_COL] = pdf[GA.GROUP_COL].fillna("n/a")
    if metric._time_group not in [
        M.TimeGroup.HOUR,
        M.TimeGroup.MINUTE,
        M.TimeGroup.SECOND,
        M.TimeGroup.TOTAL,
    ]:
        pdf[GA.DATETIME_COL] = pdf[GA.DATETIME_COL].dt.date
    return pdf


def get_retention_mapping(pdf: pd.DataFrame, metric: M.RetentionMetric) -> pd.DataFrame:
    if metric._time_group == M.TimeGroup.TOTAL:
        mapping = {
            GA.RETENTION_INDEX: C.X_AXIS_COL,
            GA.AGG_VALUE_COL: C.Y_AXIS_COL,
            GA.GROUP_COL: C.COLOR_COL,
        }
    else:
        if metric._group_by is not None:
            raise Exception(
                "Break downs are not supported for retention over time metric"
            )
        mapping = {
            GA.RETENTION_INDEX: C.X_AXIS_COL,
            GA.AGG_VALUE_COL: C.Y_AXIS_COL,
            GA.DATETIME_COL: C.COLOR_COL,
        }

    return pdf.rename(columns=mapping)
