from __future__ import annotations

from typing import Any, List, cast

import mitzu.adapters.databricks_sqlalchemy.sqlalchemy.datatype as DA_T
import mitzu.model as M
import sqlalchemy as SA
from mitzu.adapters.databricks_sqlalchemy import sqlalchemy  # noqa: F401
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter


class DatabricksAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def get_engine(self) -> Any:
        con = self.source.connection
        if self._engine is None:
            if con.url is None:
                url = self._get_connection_url(con)
            else:
                url = con.url
            http_path = con.extra_configs.get("http_path")
            if http_path is None:
                raise Exception(
                    "Connection extra_configs must contain http_path. (extra_configs={'http_path':'<path>'}"
                )
            self._engine = SA.create_engine(url, connect_args={"http_path": http_path})
        return self._engine

    def map_type(self, sa_type: Any) -> M.DataType:
        if isinstance(sa_type, DA_T.MAP):
            return M.DataType.MAP
        if isinstance(sa_type, DA_T.STRUCT):
            return M.DataType.STRUCT
        return super().map_type(sa_type)

    def _parse_map_type(
        self, sa_type: Any, name: str, event_data_table: M.EventDataTable
    ) -> M.Field:
        print(f"Discovering map: {name}")
        map: DA_T.MAP = cast(DA_T.MAP, sa_type)
        if map.value_type in (DA_T.STRUCT, DA_T.MAP):
            raise Exception(
                f"Compounded map types are not supported: map<{map.key_type}, {map.value_type}>"
            )
        cte = self._get_dataset_discovery_cte(event_data_table)
        F = SA.func
        map_keys_func = F.array_distinct(
            F.flatten(F.collect_set(F.map_keys(cte.columns[name])))
        )

        max_cardinality = self.source.max_map_key_cardinality
        q = SA.select(
            columns=[
                SA.case(
                    [(F.size(map_keys_func) < max_cardinality, map_keys_func)],
                    else_=None,
                ).label("sub_fields")
            ]
        )
        df = self.execute_query(q)
        if df.shape[0] == 0:
            return M.Field(_name=name, _type=M.DataType.MAP)
        keys = df.iat[0, 0].tolist()
        sf_type = self.map_type(map.value_type)
        sub_fields: List[M.Field] = [M.Field(key, sf_type) for key in keys]
        return M.Field(_name=name, _type=M.DataType.MAP, _sub_fields=tuple(sub_fields))

    def _parse_complex_type(
        self, sa_type: Any, name: str, event_data_table: M.EventDataTable, path: str
    ) -> M.Field:
        if isinstance(sa_type, DA_T.STRUCT):
            struct: DA_T.STRUCT = cast(DA_T.STRUCT, sa_type)
            sub_fields: List[M.Field] = []
            for n, st in struct.attr_types:
                next_path = f"{path}.{n}"
                if next_path in event_data_table.ignored_fields:
                    continue
                sf = self._parse_complex_type(
                    sa_type=st,
                    name=n,
                    event_data_table=event_data_table,
                    path=next_path,
                )
                if sf._type == M.DataType and (
                    sf._sub_fields is None or len(sf._sub_fields) == 0
                ):
                    continue
                sub_fields.append(sf)
            return M.Field(
                _name=name, _type=M.DataType.STRUCT, _sub_fields=tuple(sub_fields)
            )
        else:
            return M.Field(_name=name, _type=self.map_type(sa_type))
