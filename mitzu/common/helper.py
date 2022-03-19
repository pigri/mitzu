from __future__ import annotations
from datetime import datetime
from typing import Any
import mitzu.common.model as ml


def parse_datetime_input(val: Any, def_val: datetime) -> datetime:
    if val is None:
        return def_val
    if type(val) == str:
        return datetime.fromisoformat(val)
    elif type(val) == datetime:
        return val
    else:
        raise ValueError(f"Invalid argument type for datetime parse: {type(val)}")


def get_segment_event_datasource(segment: ml.Segment) -> ml.EventDataSource:
    if isinstance(segment, ml.SimpleSegment):
        left = segment._left
        if isinstance(left, ml.EventDef):
            return left._source
        elif isinstance(left, ml.EventFieldDef):
            return left._source
        else:
            raise ValueError(f"Segment's left value is of invalid type: {type(left)}")
    elif isinstance(segment, ml.ComplexSegment):
        return get_segment_event_datasource(segment._left)
    else:
        raise ValueError(f"Segment is of invalid type: {type(segment)}")
