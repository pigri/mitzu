import re
import pandas as pd  # type: ignore
from typing import List, Dict


def assert_sql(expected: str, actual: str):

    expected = re.sub(r"\s+", " ", expected).lower()
    actual = re.sub(r"\s+", " ", actual).lower()

    assert expected.strip() == actual.strip()


def is_similar(a, b) -> bool:
    if type(a) == float:
        return abs(a - b) < 0.01
        # Todo add more rules
    return a == b


def assert_row(df: pd.DataFrame, **kwargs):
    records: List[Dict] = df.to_dict("records")
    if len(records) == 0:
        assert False, f"Empty dataframe for matching {kwargs}"

    closest = {}
    closest_match = -1
    for record in records:
        match = 0
        for key, val in kwargs.items():
            if is_similar(record[key], val):
                match += 1
        if closest_match == len(kwargs) and match == closest_match:
            assert False, f"Multiple records match for {kwargs}"

        if match > closest_match:
            closest_match = match
            closest = record

    if closest_match == len(kwargs):
        assert True, f"Matching record for {kwargs}"
        return

    assert False, f"Not matching record for {kwargs}\nClosest records:\n{closest}"
