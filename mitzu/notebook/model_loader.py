from __future__ import annotations

import re
from copy import copy
from typing import Any, Dict, cast

import mitzu.common.model as M

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


def _any_of(self: M.EventFieldDef, *vals: Any) -> M.SimpleSegment:
    return M.SimpleSegment(_left=self, _operator=M.Operator.ANY_OF, _right=vals)


def _not_any_of(self: M.EventFieldDef, *vals: Any) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.NONE_OF,
        _right=vals,
    )


def _like(self: M.EventFieldDef, val: str) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.LIKE,
        _right=val,
    )


def _not_like(self: M.EventFieldDef, val: str) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.NOT_LIKE,
        _right=val,
    )


def _eq(self: M.EventFieldDef, val: Any) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.EQ,
        _right=val,
    )


def _not_eq(self: M.EventFieldDef, val: Any) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.NEQ,
        _right=val,
    )


def _gt(self: M.EventFieldDef, val: Any) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.GT,
        _right=val,
    )


def _lt(self: M.EventFieldDef, val: Any) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.LT,
        _right=val,
    )


def _gt_eq(self: M.EventFieldDef, val: Any) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.GT_EQ,
        _right=val,
    )


def _lt_eq(self: M.EventFieldDef, val: Any) -> M.SimpleSegment:
    return M.SimpleSegment(
        _left=self,
        _operator=M.Operator.LT_EQ,
        _right=val,
    )


def add_enum_defs(
    event_field: M.EventFieldDef,
    class_def: Dict,
):
    if event_field._enums is not None:
        for enm in event_field._enums:
            class_def[f"{fix_def('is_'+str(enm))}"] = M.SimpleSegment(
                _left=event_field,
                _operator=M.Operator.EQ,
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
        event_field: M.EventFieldDef,
    ):
        event_field = copy(event_field)
        class_def: Dict[str, Any] = {}
        field_type = event_field._field._type

        if field_type == M.DataType.STRING:
            class_def["like"] = _like
            class_def["not_like"] = _not_like
            add_enum_defs(event_field, class_def)

        if field_type == M.DataType.BOOL:
            class_def["is_true"] = M.SimpleSegment(
                _left=event_field,
                _operator=M.Operator.EQ,
                _right=True,
            )
            class_def["is_false"] = M.SimpleSegment(
                _left=event_field,
                _operator=M.Operator.EQ,
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

        class_def["is_null"] = M.SimpleSegment(
            _left=event_field,
            _operator=M.Operator.EQ,
            _right=None,
        )
        class_def["is_not_null"] = M.SimpleSegment(
            _left=event_field,
            _operator=M.Operator.NEQ,
            _right=None,
        )

        return type(
            f"_{event_name}_{fix_def(event_field._field._name)}",
            (M.EventFieldDef,),
            class_def,
        )

    def _create_event_instance(self, event: M.EventDef, source: M.EventDataSource):
        fields = event._fields

        class_def: Dict[str, Any] = {}
        for event_field in fields.values():
            field_class = self._create_event_field_class(event._event_name, event_field)
            class_instance = field_class(
                _event_name=event._event_name,
                _field=event_field._field,
                _source=source,
                _event_data_table=event_field._event_data_table,
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

        return type(f"_{event._event_name}", (M.SimpleSegment,), class_def)(event)

    def _process_schema(self, discovered_dataset: M.DiscoveredEventDataSource):
        source = discovered_dataset.source
        class_def = {}
        for ed_table in source.event_data_tables:
            definitions = discovered_dataset.definitions[ed_table]
            for event_name, event_def in definitions.items():
                fixed_name = fix_def(event_name)
                class_def[fixed_name] = self._create_event_instance(event_def, source)
        return class_def

    def create_datasource_class_model(
        self, defs: M.DiscoveredEventDataSource
    ) -> DatasetModel:
        class_def = self._process_schema(defs)
        return cast(DatasetModel, type("_dataset_model", (DatasetModel,), class_def))
