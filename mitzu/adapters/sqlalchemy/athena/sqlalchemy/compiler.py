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

from mitzu.adapters.sqlalchemy.athena.sqlalchemy.error import AthenaQueryError

from sqlalchemy.sql import compiler

# https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html
ATHENA_RESERVED_WORDS = {
    "ALL",
    "ALTER",
    "AND",
    "ARRAY",
    "AS",
    "AUTHORIZATION",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BOOLEAN",
    "BOTH",
    "BY",
    "CASE",
    "CASHE",
    "CAST",
    "CHAR",
    "COLUMN",
    "CONF",
    "CONSTRAINT",
    "COMMIT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "CURSOR",
    "DATABASE",
    "DATE",
    "DAYOFWEEK",
    "DECIMAL",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "EXCHANGE",
    "EXISTS",
    "EXTENDED",
    "EXTERNAL",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FLOAT",
    "FLOOR",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "FUNCTION",
    "GRANT",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IF",
    "IMPORT",
    "IN",
    "INNER",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LESS",
    "LIKE",
    "LOCAL",
    "MACRO",
    "MAP",
    "MORE",
    "NONE",
    "NOT",
    "NULL",
    "NUMERIC",
    "OF",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OVER",
    "PARTIALSCAN",
    "PARTITION",
    "PERCENT",
    "PRECEDING",
    "PRECISION",
    "PRESERVE",
    "PRIMARY",
    "PROCEDURE",
    "RANGE",
    "READS",
    "REDUCE",
    "REGEXP",
    "REFERENCES",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "SELECT",
    "SET",
    "SMALLINT",
    "START,TABLE",
    "TABLESAMPLE",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TO",
    "TRANSFORM",
    "TRIGGER",
    "TRUE",
    "TRUNCATE",
    "UNBOUNDED,UNION",
    "UNIQUEJOIN",
    "UPDATE",
    "USER",
    "USING",
    "UTC_TIMESTAMP",
    "VALUES",
    "VARCHAR",
    "VIEWS",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "ALTER",
    "AND",
    "AS",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT_DATE",
    "CURRENT_PATH",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEALLOCATE",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXECUTE",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FIRST",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IN",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "LAST",
    "LEFT",
    "LIKE",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NATURAL",
    "NORMALIZE",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PREPARE",
    "RECURSIVE",
    "RIGHT",
    "ROLLUP",
    "SELECT",
    "TABLE",
    "THEN",
    "TRUE",
    "UNESCAPE",
    "UNION",
    "UNNEST",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WITH",
}


class AthenaSQLCompiler(compiler.SQLCompiler):
    def limit_clause(self, select, **kw):
        if select._offset_clause is not None:
            raise AthenaQueryError("OFFSET is not supported")
        if select._limit_clause is not None:
            return "\nLIMIT " + self.process(select._limit_clause, **kw)
        return ""


class AthenaDDLCompiler(compiler.DDLCompiler):
    pass


class AthenaTypeCompiler(compiler.GenericTypeCompiler):
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


class AthenaIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = ATHENA_RESERVED_WORDS
