from __future__ import annotations
import services.common.model as M
import services.adapters.generic_adapter as GA


def create_adapter(source: M.EventDataSource) -> GA.GenericDatasetAdapter:
    con_type = source.connection.connection_type
    if con_type == M.ConnectionType.FILE:
        from services.adapters.sqlite_adapter import SQLiteAdapter

        return SQLiteAdapter(source)

    raise NotImplementedError(con_type)
