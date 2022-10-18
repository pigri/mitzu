from __future__ import annotations


import mitzu.model as M
from mitzu.adapters.sqlalchemy_adapter import SQLAlchemyAdapter


class SnowflakeAdapter(SQLAlchemyAdapter):
    def _get_connection_url(self, con: M.Connection):
        url = super()._get_connection_url(con)
        extra_args = {}

        if "warehouse" in con.extra_configs.keys():
            extra_args["warehouse"] = con.extra_configs["warehouse"]

        if "role" in con.extra_configs.keys():
            extra_args["role"] = con.extra_configs["role"]

        if len(extra_args) > 0:
            url += "?" + "&".join(
                f"{key}={value}" for (key, value) in extra_args.items()
            )

        return url
