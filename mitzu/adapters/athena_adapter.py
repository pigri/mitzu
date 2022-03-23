from __future__ import annotations
from typing import Any, List

from mitzu.adapters.slqalchemy_adapter import SQLAlchemyAdapter
import mitzu.common.model as M
from urllib.parse import quote_plus  # PY2: from urllib import quote_plus
from sqlalchemy.engine import create_engine


class AthenaAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def get_engine(self) -> Any:
        if self._engine is None:
            params = self.source.connection.connection_params
            conn_str = (
                "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}"
                "@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}"
            )

            engine = create_engine(
                conn_str.format(
                    aws_access_key_id=quote_plus(params["aws_access_key_id"]),
                    aws_secret_access_key=quote_plus(params["aws_secret_access_key"]),
                    region_name=quote_plus(params["region_name"]),
                    schema_name=quote_plus(params.get("schema_name", "default")),
                    s3_staging_dir=quote_plus(params["s3_staging_dir"]),
                )
            )
            #
            engine.dialect.description_encoding = None

            self._engine = engine
        return self._engine
