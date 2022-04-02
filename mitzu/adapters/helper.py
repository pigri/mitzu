from datetime import datetime
import pandas as pd  # type: ignore
import mitzu.adapters.generic_adapter as GA
from typing import Optional


def str_to_datetime(val: str) -> Optional[datetime]:
    if val is None:
        return None
    return datetime.fromisoformat(val)


def dataframe_str_to_datetime(pdf: pd.DataFrame, column: str) -> pd.DataFrame:
    pdf[column] = pdf[column].apply(str_to_datetime)
    return pdf


def pdf_string_array_to_array(
    pdf: pd.DataFrame, split_text: str = ", ", omit_chars: int = 2
):
    for col in pdf.columns:
        if col != GA.EVENT_NAME_ALIAS_COL:
            pdf[col] = pdf[col].apply(
                lambda val: [v for v in val[omit_chars:-omit_chars].split(split_text)]
                if type(val) == str
                else val
            )
    return pdf
