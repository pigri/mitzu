# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pyathena
import pyathena.connection as pa_conn
from mitzu.adapters.sqlalchemy.athena.sqlalchemy import compiler, datatype, error
from pyathena.model import AthenaTableMetadataColumn

from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.url import URL


class AthenaDialect(DefaultDialect):
    name = "athena"
    driver = "rest"

    statement_compiler = compiler.AthenaSQLCompiler
    ddl_compiler = compiler.AthenaDDLCompiler
    type_compiler = compiler.AthenaTypeCompiler
    preparer = compiler.AthenaIdentifierPreparer

    # Data Type
    supports_native_enum = False
    supports_native_boolean = True
    supports_native_decimal = True

    # Column options
    supports_sequences = False
    supports_comments = True
    inline_comments = True
    supports_default_values = False

    # DDL
    supports_alter = False

    # DML
    # Queries of the form `INSERT () VALUES ()` is not supported by Athena.
    supports_empty_insert = False
    supports_multivalues_insert = True
    postfetch_lastrowid = False

    # Caching
    # Warnings are generated by SQLAlchmey if this flag is not explicitly set
    # and tests are needed before being enabled
    supports_statement_cache = False

    @classmethod
    def dbapi(cls):
        return pyathena

    def create_connect_args(self, url: URL) -> Tuple[Sequence[Any], Mapping[str, Any]]:
        args: Sequence[Any] = list()
        kwargs: Dict[str, Any] = dict(server_hostname=url.host)

        if url.port:
            kwargs["port"] = url.port

        db_parts = (url.database or "system").split("/")

        if len(db_parts) == 1:
            kwargs["schema"] = db_parts[0]
        elif len(db_parts) == 2:
            kwargs["catalog"] = db_parts[0]
            kwargs["schema"] = db_parts[1]
        else:
            raise ValueError(f"Unexpected database format {url.database}")

        if url.password:
            kwargs["aws_access_key_id"] = url.username
            kwargs["aws_secret_access_key"] = url.password

        if "s3_staging_dir" in url.query:
            kwargs["s3_staging_dir"] = url.query["s3_staging_dir"]

        if "work_group" in url.query:
            kwargs["work_group"] = url.query["work_group"]

        kwargs["region_name"] = url.query["region"]
        kwargs["session"] = None

        return args, kwargs

    def get_columns(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> List[Dict[str, Any]]:

        db_conn: pa_conn.Connection = connection._dbapi_connection
        schema, table_name = self._handle_table_name(table_name, schema)

        res: List[AthenaTableMetadataColumn] = (
            db_conn.cursor()
            .get_table_metadata(
                catalog_name=None, schema_name=schema, table_name=table_name
            )
            .columns
        )
        columns = []
        for record in res:
            if record.type is None:
                raise error.AthenaQueryError(f"'{record.name}' column type is unkown")
            column = dict(
                name=record.name,
                type=datatype.parse_sqltype(record.type),
                nullable=True,
                default=None,
            )
            columns.append(column)
        return columns

    def get_pk_constraint(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> Dict[str, Any]:
        """Athena has no support for primary keys. Returns a dummy"""
        return dict(name=None, constrained_columns=[])

    def get_primary_keys(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> List[str]:
        pk = self.get_pk_constraint(connection, table_name, schema)
        return pk.get("constrained_columns")  # type: ignore

    def get_foreign_keys(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> List[Dict[str, Any]]:
        """Athena has no support for foreign keys. Returns an empty list."""
        return []

    def get_schema_names(self, connection: Connection, **kw) -> List[str]:
        query = "show schemas"
        res = connection.execute(query)
        return [row.databaseName for row in res]

    def get_table_names(
        self, connection: Connection, schema: str = None, **kw
    ) -> List[str]:
        schema = schema or self._get_default_schema_name(connection)
        query = f"show tables in {schema}"
        res = connection.execute(query)
        return [row.tableName for row in res]

    def get_temp_table_names(
        self, connection: Connection, schema: str = None, **kw
    ) -> List[str]:
        """Athena has no support for listing temporary tables. Returns an empty list."""
        return []

    def get_view_names(
        self, connection: Connection, schema: str = None, **kw
    ) -> List[str]:
        schema = schema or self._get_default_schema_name(connection)
        query = f"show views in {schema}"
        res = connection.execute(query)
        return [row.viewName for row in res]

    def get_temp_view_names(
        self, connection: Connection, schema: str = None, **kw
    ) -> List[str]:
        """Athena has no support for temporary views. Returns an empty list."""
        return []

    def get_view_definition(
        self, connection: Connection, view_name: str, schema: str = None, **kw
    ) -> str:
        schema, view_name = self._handle_table_name(view_name, schema)

        query = f"show create table {schema}.{view_name}"
        return connection.execute(query).scalar()

    def get_indexes(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> List[Dict[str, Any]]:
        """Athena has no support for indexes"""
        return []

    def get_sequence_names(
        self, connection: Connection, schema: str = None, **kw
    ) -> List[str]:
        """Athena has no support for sequences. Returns an empty list."""
        return []

    def get_unique_constraints(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> List[Dict[str, Any]]:
        """Athena has no support for unique constraints. Returns an empty list."""
        return []

    def get_check_constraints(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> List[Dict[str, Any]]:
        """Athena has no support for check constraints. Returns an empty list."""
        return []

    def get_table_comment(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> Dict[str, Any]:
        return {}

    def has_schema(self, connection: Connection, schema: str) -> bool:
        return schema in self.get_schema_names(connection)

    def has_table(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> bool:
        schema, table_name = self._handle_table_name(table_name, schema)
        rows = connection.cursor().tables().fetchall()
        for row in rows:
            if (
                row.TABLE_NAME.lower() == table_name.lower()
                and row.TABLE_SCHEM.lower() == schema.lower()
            ):
                return True

        return False

    def has_sequence(
        self, connection: Connection, sequence_name: str, schema: str = None, **kw
    ) -> bool:
        """Athena has no support for sequence. Returns False indicate that given sequence does not exists."""
        return False

    def do_execute(
        self,
        cursor: Any,
        statement: str,
        parameters: Tuple[Any, ...],
        context: DefaultExecutionContext = None,
    ):
        cursor.execute(statement, parameters)

    def do_rollback(self, athena_connection: Connection):
        pass

    def set_isolation_level(self, athena_conn: Connection, level: str) -> None:
        pass

    def get_isolation_level(self, athena_conn: Connection) -> str:
        return "NONE"

    def get_default_isolation_level(self, athena_conn: Connection) -> str:
        return "NONE"  # TBD verify this is the correct value

    def _get_default_schema_name(self, connection: Connection) -> str:
        return ""

    def _handle_table_name(
        self, table_name: str, schema: Optional[str] = None
    ) -> Tuple[str, str]:
        return self._split_table_name(self._get_full_table(table_name, schema))

    def _split_table_name(self, full_table: str) -> Tuple[str, str]:
        parts = full_table.split(".")
        if len(parts) != 2:
            raise ValueError(f"Table name {full_table} is invalid.")
        return (parts[0], parts[1])

    def _get_full_table(
        self, table_name: str, schema: str = None, quote: bool = False
    ) -> str:
        res = (
            self.identifier_preparer.quote_identifier(table_name)
            if quote
            else table_name
        )
        if schema:
            schema_part = (
                self.identifier_preparer.quote_identifier(schema) if quote else schema
            )
            res = f"{schema_part}.{res}"

        if len(res.split(".")) == 3:
            raise ValueError(f"Table name {res} is invalid.")
        return res
