import mitzu.common.model as M
from typing import Any

from mitzu.adapters.slqalchemy_adapter import SQLAlchemyAdapter  # type: ignore
import sqlalchemy as SA  # type: ignore


class PostgresAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def get_engine(self) -> Any:
        params = self.source.connection.connection_params
        if self._engine is not None and not params.get("cache_engine", True):
            self._engine.dispose()
            self._engine = None

        if self._engine is None:
            user_name = params["user_name"]
            password = params["password"]
            host = params["host"]
            port = params.get("port", 5432)
            schema = params.get("schema", "default")
            self._engine = SA.create_engine(
                f"postgresql://{user_name}:{password}@{host}:{port}/{schema}"
            )
        return self._engine
