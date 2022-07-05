from __future__ import annotations

import logging
import os
from typing import Any, List

import awsgi
import boto3
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.webapp.webapp as MWA
from dash import Dash

S3_BUCKET = os.getenv("MITZU_MODELS_BUCKET", None)
MODEL_PATH = os.getenv("MITZU_MODEL", None)

logging.getLogger().setLevel(logging.INFO)


def read_project_from_s3() -> bytes:
    logging.info(f"Reading model from {S3_BUCKET}/{MODEL_PATH}")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(S3_BUCKET)
    obj = bucket.Object(MODEL_PATH)
    return obj.get()["Body"].read()


class SimplePersistencyProvider(MWA.PersistencyProvider):
    def __init__(
        self, discovered_dataset: M.DiscoveredEventDataSource, project_name: str
    ) -> None:
        self._discovered_dataset = discovered_dataset
        self._project_name = project_name

    def list_keys(self, path: str) -> List[str]:
        return [self._project_name]

    def get_item(self, key: str) -> Any:
        return self._discovered_dataset

    def delete_item(self, key: str) -> None:
        pass

    def upsert_item(self, key: str, item: Any) -> None:
        pass


def create_app():
    logging.info(f"Creating Mitzu Webapp ------------------------------------------")
    app = Dash(
        __name__,
        compress=False,
        external_stylesheets=[
            dbc.themes.MINTY,
            dbc.icons.BOOTSTRAP,
            "assets/layout.css",
            "assets/components.css",
        ],
        title="Mitzu",
        suppress_callback_exceptions=True,
    )
    app._favicon = "assets/favicon_io/favicon.ico"
    project_binary = read_project_from_s3()
    deds = M.DiscoveredEventDataSource.load_from_project_binary(project_binary)
    pp = SimplePersistencyProvider(deds, "demo_athena_project")

    webapp = MWA.MitzuWebApp(
        persistency_provider=pp,
        app=app,
    )
    webapp.init_app()
    logging.info(f"Finished initialization")
    return app


app = create_app()


def handler(event, context):
    return awsgi.response(app.server, event, context)
