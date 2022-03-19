from __future__ import annotations
import services.common.model as M
import services.adapters.generic_adapter as GA


def get_or_create_adapter(source: M.EventDataSource) -> GA.GenericDatasetAdapter:
    if source.adapter is None:
        con_type = source.connection.connection_type
        if con_type == M.ConnectionType.FILE:
            from services.adapters.sqlite_adapter import SQLiteAdapter

            source.adapter = SQLiteAdapter(source)
        else:
            raise NotImplementedError(con_type)

    return source.adapter
