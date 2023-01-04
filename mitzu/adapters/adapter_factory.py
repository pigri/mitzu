from __future__ import annotations

import mitzu.adapters.generic_adapter as GA
import mitzu.model as M


def create_adapter(project: M.Project) -> GA.GenericDatasetAdapter:
    con_type = project.connection.connection_type
    res: GA.GenericDatasetAdapter
    if con_type == M.ConnectionType.FILE:
        from mitzu.adapters.file_adapter import FileAdapter

        res = FileAdapter(project)
    elif con_type == M.ConnectionType.SQLITE:
        from mitzu.adapters.file_adapter import SQLiteAdapter

        res = SQLiteAdapter(project)
    elif con_type == M.ConnectionType.ATHENA:
        from mitzu.adapters.athena_adapter import AthenaAdapter

        res = AthenaAdapter(project)
    elif con_type == M.ConnectionType.MYSQL:
        from mitzu.adapters.mysql_adapter import MySQLAdapter

        res = MySQLAdapter(project)
    elif con_type == M.ConnectionType.POSTGRESQL:
        from mitzu.adapters.postgresql_adapter import PostgresqlAdapter

        res = PostgresqlAdapter(project)
    elif con_type == M.ConnectionType.TRINO:
        from mitzu.adapters.trino_adapter import TrinoAdapter

        res = TrinoAdapter(project)
    elif con_type == M.ConnectionType.DATABRICKS:
        from mitzu.adapters.databricks_adapter import DatabricksAdapter

        res = DatabricksAdapter(project)
    elif con_type == M.ConnectionType.SNOWFLAKE:
        from mitzu.adapters.snowflake_adapter import SnowflakeAdapter

        res = SnowflakeAdapter(project)
    else:
        from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter

        res = SQLAlchemyAdapter(project)

    return res
