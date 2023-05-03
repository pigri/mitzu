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


def get_metric_group_by(metric: M.Metric) -> Optional[M.EventFieldDef]:
    if isinstance(metric, M.SegmentationMetric):
        return metric._segment._group_by
    elif (
        isinstance(metric, M.ConversionMetric) and len(metric._conversion._segments) > 0
    ):
        return metric._conversion._segments[0]._group_by
    elif isinstance(metric, M.RetentionMetric):
        return metric._initial_segment._group_by
    return None


def create_unique_id():
    return str(uuid4())[-12:]
