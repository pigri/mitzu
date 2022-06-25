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

from mitzu.adapters.databricks_sqlalchemy.sqlalchemy.error import DatabricksQueryError

from sqlalchemy.sql import compiler

# https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-reserved-words.html
DB_RESERVED_WORDS = {
    #  reserves words
    "anti",
    "cross",
    "except",
    "full",
    "inner",
    "intersect",
    "join",
    "lateral",
    "left",
    "minus",
    "natural",
    "on",
    "right",
    "semi",
    "union",
    "using",
    # expression special words
    "null",
    "default",
    # reserved scheman words
    "builtin",
    "session",
    "information_schema",
    "sys",
    # other
    "all",
    "alter",
    "and",
    "any",
    "array",
    "as",
    "at",
    "authorization",
    "between",
    "both",
    "by",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "commit",
    "constraint",
    "create",
    "cross",
    "cube",
    "current",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "delete",
    "describe",
    "distinct",
    "drop",
    "else",
    "end",
    "escape",
    "except",
    "exists",
    "external",
    "extract",
    "false",
    "fetch",
    "filter",
    "for",
    "foreign",
    "from",
    "full",
    "function",
    "global",
    "grant",
    "group",
    "grouping",
    "having",
    "in",
    "inner",
    "insert",
    "intersect",
    "interval",
    "into",
    "is",
    "join",
    "leading",
    "left",
    "like",
    "local",
    "natural",
    "no",
    "not",
    "null",
    "of",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "partition",
    "position",
    "primary",
    "range",
    "references",
    "revoke",
    "right",
    "rollback",
    "rollup",
    "row",
    "rows",
    "select",
    "session_user",
    "set",
    "some",
    "start",
    "table",
    "tablesample",
    "then",
    "time",
    "to",
    "trailing",
    "true",
    "truncate",
    "union",
    "unique",
    "unknown",
    "update",
    "user",
    "using",
    "values",
    "when",
    "where",
    "window",
    "with",
}


class DatabricksSQLCompiler(compiler.SQLCompiler):
    def limit_clause(self, select, **kw):
        if select._offset_clause is not None:
            raise DatabricksQueryError("OFFSET is not supported")
        if select._limit_clause is not None:
            return "\nLIMIT " + self.process(select._limit_clause, **kw)
        return ""


class DatabricksDDLCompiler(compiler.DDLCompiler):
    pass


class DatabricksTypeCompiler(compiler.GenericTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        precision = type_.precision or 32
        if 0 <= precision <= 32:
            return self.visit_REAL(type_, **kw)
        elif 32 < precision <= 64:
            return self.visit_DOUBLE(type_, **kw)
        else:
            raise ValueError(
                f"type.precision must be in range [0, 64], got {type_.precision}"
            )

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        return self.visit_DECIMAL(type_, **kw)

    def visit_NCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_TEXT(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_BINARY(self, type_, **kw):
        return self.visit_VARBINARY(type_, **kw)

    def visit_CLOB(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_NCLOB(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_BLOB(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, **kw)


class DatabricksIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = DB_RESERVED_WORDS

    def __init__(
        self,
        dialect,
        initial_quote='"',
        final_quote=None,
        escape_quote='"',
        quote_case_sensitive_collations=True,
        omit_schema=False,
    ):
        super().__init__(
            dialect=dialect,
            initial_quote="",
            final_quote=None,
            escape_quote="",
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )
