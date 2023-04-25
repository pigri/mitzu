from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any, Optional

import mitzu.model as M
from uuid import uuid4

LOGGER = logging.getLogger(name="mitzu_logger")
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOGGER.setLevel(os.getenv("LOG_LEVEL", logging.INFO))


def value_to_label(value: str) -> str:
    return value.title().replace("_", " ")


def parse_datetime_input(val: Any, def_val: Optional[datetime]) -> Optional[datetime]:
    if val is None:
        return def_val
    if type(val) == str:
        return datetime.fromisoformat(val)
    elif type(val) == datetime:
        return val
    else:
        raise ValueError(f"Invalid argument type for datetime parse: {type(val)}")


def get_segment_project(segment: M.Segment) -> M.Project:
    if isinstance(segment, M.SimpleSegment):
        left = segment._left
        if isinstance(left, M.EventDef):
            return left._project
        elif isinstance(left, M.EventFieldDef):
            return left._project
        else:
            raise ValueError(f"Segment's left value is of invalid type: {type(left)}")
    elif isinstance(segment, M.ComplexSegment):
        return get_segment_project(segment._left)
    else:
        raise ValueError(f"Segment is of invalid type: {type(segment)}")


def create_unique_id():
    return str(uuid4())[-12:]
