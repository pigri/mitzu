from __future__ import annotations

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M


def create_adapter(project: M.Project) -> GA.GenericDatasetAdapter:
    con_type = project.connection.connection_type
    if con_type == M.ConnectionType.FILE:
        from mitzu.adapters.file_adapter import FileAdapter

        return FileAdapter(project)
    elif con_type == M.ConnectionType.SQLITE:
        from mitzu.adapters.file_adapter import SQLiteAdapter

        return SQLiteAdapter(project)
    elif con_type == M.ConnectionType.ATHENA:
        from mitzu.adapters.athena_adapter import AthenaAdapter

        return AthenaAdapter(project)
    elif con_type == M.ConnectionType.MYSQL:
        from mitzu.adapters.mysql_adapter import MySQLAdapter

        return MySQLAdapter(project)
    elif con_type == M.ConnectionType.POSTGRESQL:
        from mitzu.adapters.postgresql_adapter import PostgresqlAdapter

        return PostgresqlAdapter(project)
    elif con_type == M.ConnectionType.TRINO:
        from mitzu.adapters.trino_adapter import TrinoAdapter

        return TrinoAdapter(project)
    elif con_type == M.ConnectionType.DATABRICKS:
        from mitzu.adapters.databricks_adapter import DatabricksAdapter

        return DatabricksAdapter(project)
    else:
        from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter

        return SQLAlchemyAdapter(project)
