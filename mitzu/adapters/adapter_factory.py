from __future__ import annotations

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M


def create_adapter(source: M.EventDataSource) -> GA.GenericDatasetAdapter:
    con_type = source.connection.connection_type
    if con_type == M.ConnectionType.FILE:
        from mitzu.adapters.file_adapter import FileAdapter

        return FileAdapter(source)
    elif con_type == M.ConnectionType.SQLITE:
        from mitzu.adapters.file_adapter import SQLiteAdapter

        return SQLiteAdapter(source)
    elif con_type == M.ConnectionType.ATHENA:
        from mitzu.adapters.athena_adapter import AthenaAdapter

        return AthenaAdapter(source)
    elif con_type == M.ConnectionType.MYSQL:
        from mitzu.adapters.mysql_adapter import MySQLAdapter

        return MySQLAdapter(source)
    elif con_type == M.ConnectionType.POSTGRESQL:
        from mitzu.adapters.postgresql_adapter import PostgresqlAdapter

        return PostgresqlAdapter(source)
    elif con_type == M.ConnectionType.TRINO:
        from mitzu.adapters.trino_adapter import TrinoAdapter

        return TrinoAdapter(source)
    elif con_type == M.ConnectionType.DATABRICKS:
        from mitzu.adapters.databricks_adapter import DatabricksAdapter

        return DatabricksAdapter(source)
    else:
        from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter

        return SQLAlchemyAdapter(source)
