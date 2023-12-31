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
import re
from typing import Iterator, List, Tuple, Type, Union

from sqlalchemy import util
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine

SQLType = Union[TypeEngine, Type[TypeEngine]]


class DOUBLE(sqltypes.Float):
    __visit_name__ = "DOUBLE"


class MAP(TypeEngine):
    __visit_name__ = "MAP"

    def __init__(self, key_type: SQLType, value_type: SQLType):
        if isinstance(key_type, type):
            key_type = key_type()
        self.key_type: TypeEngine = key_type

        if isinstance(value_type, type):
            value_type = value_type()
        self.value_type: TypeEngine = value_type

    @property
    def python_type(self):
        return dict


class STRUCT(TypeEngine):
    __visit_name__ = "STRUCT"

    def __init__(self, attr_types: List[Tuple[str, SQLType]]):
        self.attr_types: List[Tuple[str, SQLType]] = []
        for attr_name, attr_type in attr_types:
            if isinstance(attr_type, type):
                attr_type = attr_type()
            self.attr_types.append((attr_name, attr_type))

    @property
    def python_type(self):
        return list


# https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-datatypes.html
_type_map = {
    "string": sqltypes.VARCHAR,
    "varchar": sqltypes.VARCHAR,
    "char": sqltypes.CHAR,
    # === Boolean ===
    "boolean": sqltypes.BOOLEAN,
    # === Integer ===
    "tinyint": sqltypes.SMALLINT,
    "smallint": sqltypes.SMALLINT,
    "int": sqltypes.INTEGER,
    "integer": sqltypes.INTEGER,
    "bigint": sqltypes.BIGINT,
    "float": sqltypes.FLOAT,
    "double": DOUBLE,
    "decimal": sqltypes.DECIMAL,
    # === Date and time ===
    "date": sqltypes.DATE,
    "timestamp": sqltypes.TIMESTAMP,
    # 'interval year to month':
    # 'interval day to second':
    #
    # === Structural ===
    # 'array': ARRAY,
    "map": MAP,
    "struct": STRUCT
    #
}


def unquote(string: str, quote: str = '"', escape: str = "\\") -> str:
    """
    If string starts and ends with a quote, unquote it
    """
    if string.startswith(quote) and string.endswith(quote):
        string = string[1:-1]
        string = string.replace(f"{escape}{quote}", quote).replace(
            f"{escape}{escape}", escape
        )
    return string


def aware_split(
    string: str,
    delimiter: str = ",",
    maxsplit: int = -1,
    quote: str = '"',
    escaped_quote: str = r"\"",
    open_bracket: str = "<",
    close_bracket: str = ">",
) -> Iterator[str]:
    """
    A split function that is aware of quotes and brackets/parentheses.

    :param string: string to split
    :param delimiter: string defining where to split, usually a comma or space
    :param maxsplit: Maximum number of splits to do. -1 (default) means no limit.
    :param quote: string, either a single or a double quote
    :param escaped_quote: string representing an escaped quote
    :param open_bracket: string, either [, {, < or (
    :param close_bracket: string, either ], }, > or )
    """
    parens = 0
    quotes = False
    i = 0
    if maxsplit < -1:
        raise ValueError(f"maxsplit must be >= -1, got {maxsplit}")
    elif maxsplit == 0:
        yield string
        return
    for j, character in enumerate(string):
        complete = parens == 0 and not quotes
        if complete and character == delimiter:
            if maxsplit != -1:
                maxsplit -= 1
            yield string[i:j]
            i = j + len(delimiter)
            if maxsplit == 0:
                break
        elif character == open_bracket:
            parens += 1
        elif character == close_bracket:
            parens -= 1
        elif character == quote:
            left = j - len(escaped_quote) + 1
            right = j + 1
            if quotes and string[left:right] != escaped_quote:
                quotes = False
            elif not quotes:
                quotes = True
    yield string[i:]


def parse_sqltype(type_str: str) -> TypeEngine:
    type_str = type_str.strip().lower()
    match = re.match(r"^(?P<type>\w+)\s*(?:\<(?P<options>.*)\>)?", type_str)
    if not match:
        util.warn(f"Could not parse type name '{type_str}'")
        return sqltypes.NULLTYPE
    type_name = match.group("type")
    type_opts = match.group("options")

    if type_name == "array":
        item_type = parse_sqltype(type_opts)
        if isinstance(item_type, sqltypes.ARRAY):
            # Multi-dimensions array is normalized in SQLAlchemy, e.g:
            # `ARRAY(ARRAY(INT))` in Spark SQL will become `ARRAY(INT(), dimensions=2)` in SQLAlchemy
            dimensions = (item_type.dimensions or 1) + 1
            return sqltypes.ARRAY(item_type.item_type, dimensions=dimensions)
        return sqltypes.ARRAY(item_type)
    elif type_name == "map":
        key_type_str, value_type_str = aware_split(type_opts)
        key_type = parse_sqltype(key_type_str)
        value_type = parse_sqltype(value_type_str)
        return MAP(key_type, value_type)
    elif type_name == "struct":
        attr_types: List[Tuple[str, SQLType]] = []
        for attr in aware_split(type_opts):
            attr_name, attr_type_str = aware_split(
                attr.strip(), delimiter=":", maxsplit=1
            )
            attr_name = unquote(attr_name)
            attr_type = parse_sqltype(attr_type_str)
            attr_types.append((attr_name, attr_type))
        return STRUCT(attr_types)

    if type_name not in _type_map:
        util.warn(f"Did not recognize type '{type_name}'")
        return sqltypes.NULLTYPE
    type_class = _type_map[type_name]
    type_args = [int(o.strip()) for o in type_opts.split(",")] if type_opts else []
    if type_name in ("time", "timestamp"):
        type_kwargs = dict(timezone=type_str.endswith("with time zone"))
        return type_class(**type_kwargs)
    return type_class(*type_args)
