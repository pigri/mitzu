from __future__ import annotations

import base64
import json
import zlib
from datetime import datetime
from typing import Any, Dict, Optional, Union

import mitzu.model as M

EFD_OR_ED_TYPE = Union[M.EventFieldDef, M.EventDef]


def find_edt(eds: M.EventDataSource, event_name: str) -> M.EventDataTable:
    dd = eds._discovered_event_datasource.get_value()
    if dd is None:
        raise ValueError("EventDataSource not yet discovered")
    for edt, events in dd.definitions.items():
        if event_name in events.keys():
            return edt
    raise Exception(f"Couldn't find {event_name} in any of the datasources.")


def _to_dict(value: Any) -> Any:
    if value is None:
        return None
    if type(value) in (str, float, int, bool):
        return value
    if type(value) == datetime:
        return value.isoformat()
    if type(value) == list:
        return [_to_dict(v) for v in value]
    if type(value) == tuple:
        return [_to_dict(v) for v in value]

    if isinstance(value, M.TimeGroup):
        return value.name.lower()
    if isinstance(value, M.TimeWindow):
        return f"{value.value} {value.period.name.lower()}"
    if isinstance(value, M.Field):
        return value._get_name()
    if isinstance(value, M.MetricConfig):
        return {
            "sdt": _to_dict(value.start_dt),
            "edt": _to_dict(value.end_dt),
            "lbd": _to_dict(value.lookback_days),
            "tg": _to_dict(value.time_group),
            "mgc": _to_dict(value.max_group_count),
            "gb": _to_dict(value.group_by),
            "ct": _to_dict(value.custom_title),
        }
    if isinstance(value, M.EventDef):
        return {"en": value._event_name}
    if isinstance(value, M.EventFieldDef):
        return {"en": value._event_name, "f": _to_dict(value._field)}
    if isinstance(value, M.SimpleSegment):
        return {
            "l": _to_dict(value._left),
            "op": value._operator.name if value._operator is not None else None,
            "r": _to_dict(value._right) if value._right is not None else None,
        }
    if isinstance(value, M.ComplexSegment):
        return {
            "l": _to_dict(value._left),
            "bop": value._operator.name,
            "r": _to_dict(value._right),
        }
    if isinstance(value, M.Conversion):
        return {"segs": [_to_dict(seg) for seg in value._segments]}
    if isinstance(value, M.ConversionMetric):
        return {
            "conv": _to_dict(value._conversion),
            "cw": _to_dict(value._conv_window),
            "co": _to_dict(value._config),
        }
    if isinstance(value, M.SegmentationMetric):
        return {"seg": _to_dict(value._segment), "co": _to_dict(value._config)}

    raise ValueError("Unsupported value type: {}".format(type(value)))


def remove_nones(dc: Dict) -> Optional[Dict]:
    res = {}
    for k, v in dc.items():
        if type(v) == dict:
            v = remove_nones(v)
        if type(v) == list:
            v = [remove_nones(i) if type(i) == dict else i for i in v if i is not None]
        if v is not None:
            res[k] = v
    if len(res) == 0:
        return None
    return res


def to_dict(metric: M.Metric) -> Dict:
    res = _to_dict(metric)
    res = remove_nones(res)
    if res is None:
        raise Exception("Serialized metric definition is empty")
    return res


def _from_dict(
    value: Any,
    eds: M.EventDataSource,
    type_hint: Any,
    path: str,
    other_hint: Any = None,
) -> Any:
    if value is None:
        return None
    if type_hint is None:
        if "conv" in value:
            return _from_dict(value, eds, M.ConversionMetric, path)
        if "seg" in value:
            return _from_dict(value, eds, M.SegmentationMetric, path)
        if "segs" in value:
            return _from_dict(value, eds, M.Conversion, path)
        if "l" in value:
            return _from_dict(value, eds, M.Segment, path)
        raise ValueError(
            f"Can't deserialize metric from {value} 'conv' or 'seg' expected at {path}"
        )
    if type_hint == M.MetricConfig:
        return M.MetricConfig(
            start_dt=_from_dict(value.get("sdt"), eds, datetime, path + ".sdt"),
            end_dt=_from_dict(value.get("edt"), eds, datetime, path + ".edt"),
            lookback_days=_from_dict(
                value.get("lbd"), eds, M.TimeWindow, path + ".lbd"
            ),
            time_group=_from_dict(value.get("tg"), eds, M.TimeGroup, path + ".tg"),
            max_group_count=_from_dict(value.get("mgc"), eds, int, path + ".mgc"),
            group_by=_from_dict(value.get("gb"), eds, M.EventFieldDef, path + ".gb"),
            custom_title=_from_dict(value.get("ct"), eds, str, path + ".ct"),
        )
    if type_hint == M.ConversionMetric:
        return M.ConversionMetric(
            conversion=_from_dict(value.get("conv"), eds, M.Conversion, path + ".conv"),
            conv_window=_from_dict(value.get("cw"), eds, M.TimeWindow, path + ".cw"),
            config=_from_dict(value.get("co"), eds, M.MetricConfig, path + ".co"),
        )
    if type_hint == M.Conversion:
        return M.Conversion(
            segments=_from_dict(
                value.get("segs"), eds, list, path + ".segs", M.Segment
            ),
        )
    if type_hint == M.SegmentationMetric:
        return M.SegmentationMetric(
            segment=_from_dict(value.get("seg"), eds, M.Segment, path + ".seg"),
            config=_from_dict(value.get("co"), eds, M.MetricConfig, path + ".co"),
        )
    if type_hint == M.Segment:
        if "bop" in value:
            return M.ComplexSegment(
                _left=_from_dict(value.get("l"), eds, M.Segment, path + ".l"),
                _operator=_from_dict(
                    value.get("bop"), eds, M.BinaryOperator, path + ".bop"
                ),
                _right=_from_dict(value.get("r"), eds, M.Segment, path + ".r"),
            )
        elif "op" in value:
            return M.SimpleSegment(
                _left=_from_dict(value.get("l"), eds, EFD_OR_ED_TYPE, path + ".l"),
                _operator=_from_dict(value.get("op"), eds, M.Operator, path + ".op"),
                _right=_from_dict(value.get("r"), eds, Any, path + ".r"),
            )
        return M.SimpleSegment(
            _left=_from_dict(value.get("l"), eds, EFD_OR_ED_TYPE, path + ".l")
        )

    if type_hint == M.EventFieldDef:
        event_name = value.get("en")
        edt = find_edt(eds, event_name)
        return M.EventFieldDef(
            _event_name=_from_dict(event_name, eds, str, path + ".en"),
            _field=_from_dict(
                value.get("f"), eds, M.Field, path + ".f", other_hint=event_name
            ),
            _source=eds,
            _event_data_table=edt,
        )
    if type_hint == M.EventDef:
        event_name = value.get("en")
        edt = find_edt(eds, event_name)
        return M.EventDef(
            _event_name=_from_dict(event_name, eds, str, path + ".en"),
            _fields={},
            _source=eds,
            _event_data_table=edt,
        )
    if type_hint == EFD_OR_ED_TYPE:
        if "f" in value:
            return _from_dict(value, eds, M.EventFieldDef, path)
        else:
            return _from_dict(value, eds, M.EventDef, path)
    if type_hint == M.Operator:
        return M.Operator[value.upper()]
    if type_hint == M.BinaryOperator:
        return M.BinaryOperator[value.upper()]
    if type_hint == M.Field:
        event_name = other_hint
        edt = find_edt(eds, event_name)
        dd = eds._discovered_event_datasource.get_value()
        if dd is None:
            raise Exception(f"Couldn't find {event_name} in any of the datasources.")

        event_def: M.EventDef = dd.definitions[edt][event_name]
        fields = event_def._fields.keys()
        for f in fields:
            if f._get_name() == value:
                return f
        raise Exception(f"Couldn't find {value} among {event_name} fields.")
    if type_hint == M.TimeGroup:
        return M.TimeGroup.parse(value)
    if type_hint == M.TimeWindow:
        return M.TimeWindow.parse(value)
    if type_hint == datetime:
        return datetime.fromisoformat(value)
    if type_hint == list:
        return [
            _from_dict(val, eds, other_hint, path + f"[{i}]")
            for i, val in enumerate(value)
        ]
    if type_hint in [int, str, Any]:
        return value

    raise ValueError(f"Can't process {value} at {path} with type_hint {type_hint}")


def from_dict(value: Dict, event_data_source: M.EventDataSource) -> M.Metric:
    return _from_dict(value, event_data_source, None, "")


def to_compressed_string(metric: M.Metric) -> str:
    metric_json = json.dumps(to_dict(metric))
    zipped = zlib.compress(metric_json.encode("utf-8"), 9)
    return base64.b64encode(zipped).decode("utf-8")


def from_compressed_string(
    compressed_str: str, event_data_source: M.EventDataSource
) -> M.Metric:
    zipped = base64.b64decode(compressed_str)
    unzipped = zlib.decompress(zipped)
    val = json.loads(unzipped)
    return from_dict(val, event_data_source)
