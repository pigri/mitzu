from __future__ import annotations

from typing import Any

import mitzu.model as M
import sqlalchemy as SA
from mitzu.adapters.sqlalchemy_adapter import FieldReference, SQLAlchemyAdapter


class PostgresqlAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return field_ref + SA.text(f"interval '{timewindow.value} {timewindow.period}'")
