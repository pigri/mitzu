from __future__ import annotations
from typing import Dict, Any
import re
from copy import copy


from mitzu.common.model import (
    DataType,
    DiscoveredDataset,
    EventFieldDef,
    EventDataSource,
    EventDef,
    SimpleSegment,
    Operator,
    ANY_EVENT_NAME,
    Segment,
)

NUM_2_WORDS = {
    1: "one",
    2: "two",
    3: "three",
    4: "four",
    5: "five",
    6: "six",
    7: "seven",
    8: "eight",
    9: "nine",
}


def fix_def(val: str):
    fixed = re.sub("[^a-zA-Z0-9]", "_", val.lower())
    if fixed[0].isdigit():
        fixed = f"{NUM_2_WORDS[int(fixed[0])]}_{fixed[1:]}"
    return fixed[:32]


def _any_of(self: EventFieldDef, *vals: Any) -> SimpleSegment:
    return SimpleSegment(_left=self, _operator=Operator.ANY_OF, _right=vals)


def _not_any_of(self: EventFieldDef, *vals: Any) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.NONE_OF,
        _right=vals,
    )


def _like(self: EventFieldDef, val: str) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.LIKE,
        _right=val,
    )


def _not_like(self: EventFieldDef, val: str) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.NOT_LIKE,
        _right=val,
    )


def _eq(self: EventFieldDef, val: Any) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.EQ,
        _right=val,
    )


def _not_eq(self: EventFieldDef, val: Any) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.NEQ,
        _right=val,
    )


def _gt(self: EventFieldDef, val: Any) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.GT,
        _right=val,
    )


def _lt(self: EventFieldDef, val: Any) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.LT,
        _right=val,
    )


def _gt_eq(self: EventFieldDef, val: Any) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.GT_EQ,
        _right=val,
    )


def _lt_eq(self: EventFieldDef, val: Any) -> SimpleSegment:
    return SimpleSegment(
        _left=self,
        _operator=Operator.LT_EQ,
        _right=val,
    )


def _event_condition_constructor(self: SimpleSegment, event_def: EventDef):
    Segment.__init__(self)
    self._left = event_def


def add_enum_defs(
    event_field: EventFieldDef,
    class_def: Dict,
):
    if event_field._enums is not None:
        for enm in event_field._enums:
            class_def[f"{fix_def('is_'+str(enm))}"] = SimpleSegment(
                _left=event_field,
                _operator=Operator.EQ,
                _right=enm,
            )


class DatasetModel:
    @classmethod
    def _to_globals(cls, glbs: Dict):
        for k, v in cls.__dict__.items():
            if k != "_to_globals":
                glbs[k] = v


class ModelLoader:
    def _create_event_field_class(
        self,
        event_name: str,
        event_field: EventFieldDef,
    ):
        class_def: Dict[str, Any] = {}
        field_type = event_field._field._type

        if field_type == DataType.STRING:
            class_def["like"] = _like
            class_def["not_like"] = _not_like
            add_enum_defs(event_field, class_def)

        if field_type == DataType.BOOL:
            class_def["is_true"] = SimpleSegment(
                _left=event_field,
                _operator=Operator.EQ,
                _right=True,
            )
            class_def["is_false"] = SimpleSegment(
                _left=event_field,
                _operator=Operator.EQ,
                _right=False,
            )
        else:
            class_def["eq"] = _eq
            class_def["not_eq"] = _not_eq
            class_def["gt"] = _gt
            class_def["lt"] = _lt
            class_def["gt_eq"] = _gt_eq
            class_def["lt_eq"] = _lt_eq

            class_def["__eq__"] = _eq
            class_def["__ne__"] = _not_eq
            class_def["__gt__"] = _gt
            class_def["__lt__"] = _lt
            class_def["__ge__"] = _gt_eq
            class_def["__le__"] = _lt_eq
            class_def["not_any_of"] = _not_any_of
            class_def["any_of"] = _any_of

        class_def["is_null"] = SimpleSegment(
            _left=event_field,
            _operator=Operator.EQ,
            _right=None,
        )
        class_def["is_not_null"] = SimpleSegment(
            _left=event_field,
            _operator=Operator.NEQ,
            _right=None,
        )

        return type(
            f"_{event_name}_{fix_def(event_field._field._name)}",
            (EventFieldDef,),
            class_def,
        )

    def _create_event_instance(self, event: EventDef, source: EventDataSource):
        fields = event._fields

        class_def: Dict[str, Any] = {}
        for event_field in fields.values():
            field_class = self._create_event_field_class(event._event_name, event_field)
            class_instance = field_class(
                _event_name=event._event_name,
                _field=event_field._field,
                _source=source,
            )

            if "." in event_field._field._name:
                split = event_field._field._name.split(".")
                if len(split) > 2:
                    raise Exception(
                        "Recursively nested types are not supported (e.g. map<string, map<string,string>>)"
                    )
                inter_field_name = fix_def(split[0])
                class_field_name = fix_def(split[1])
                if inter_field_name not in class_def:
                    class_def[inter_field_name] = type(
                        f"_{event._event_name}_{inter_field_name}", (object,), {}
                    )
                setattr(class_def[inter_field_name], class_field_name, class_instance)
            else:
                class_field_name = fix_def(event_field._field._name)
                class_def[class_field_name] = class_instance
        class_def["__init__"] = _event_condition_constructor

        return type(f"_{event._event_name}", (SimpleSegment,), class_def)(event)

    def _copy_any_event(self, any_event: EventDef):
        # Any events needs to be copied so the event name can be changed to every specific event
        res = copy(any_event)
        res._fields = {}
        for f, ef in any_event._fields.items():
            res._fields[f] = copy(ef)
        return res

    def _process_schema(self, defs: DiscoveredDataset):
        schema = defs.definitions
        source = defs.source
        class_def = {}
        any_evt = schema.get(
            ANY_EVENT_NAME,
            EventDef(_event_name=ANY_EVENT_NAME, _fields={}, _source=defs.source),
        )

        for event_name, event_def in schema.items():
            any_evt_copy = self._copy_any_event(any_evt)
            event_def._fields = {**any_evt_copy._fields, **event_def._fields}
            event_def._event_name = event_name
            for _, event_field in event_def._fields.items():
                event_field._event_name = event_name
            class_def[fix_def(event_name)] = self._create_event_instance(
                event_def, source
            )
        return class_def

    def create_dataset_model(self, defs: DiscoveredDataset):
        class_def = self._process_schema(defs)
        return type("_dataset_model", (DatasetModel,), class_def)
