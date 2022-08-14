from __future__ import annotations

from typing import Any

import mitzu.model as M
from mitzu.adapters.sqlalchemy_adapter import FieldReference, SQLAlchemyAdapter

import sqlalchemy as SA


class PostgresqlAdapter(SQLAlchemyAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return field_ref + SA.text(f"interval '{timewindow.value} {timewindow.period}'")
