import pandas as pd  # type: ignore
import mitzu.adapters.generic_adapter as GA


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
