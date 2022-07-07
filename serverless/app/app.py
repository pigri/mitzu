import logging
import os
from typing import Any, List

import awsgi
import boto3
import mitzu.model as M
import mitzu.webapp.persistence as P
import mitzu.webapp.webapp as MWA

from dash_app import create_app

logging.getLogger().setLevel(logging.INFO)
S3_BUCKET = os.getenv("MITZU_MODELS_BUCKET", None)
MODEL_PATH = os.getenv("MITZU_MODEL", None)


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


def read_project_from_s3() -> bytes:
    logging.info(f"Reading model from {S3_BUCKET}/{MODEL_PATH}")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(S3_BUCKET)
    obj = bucket.Object(MODEL_PATH)
    return obj.get()["Body"].read()


def create_persistency_provider():
    access_token = os.getenv("AWS_ACCESS_KEY_ID")
    if access_token:
        project_binary = read_project_from_s3()
        deds = M.DiscoveredEventDataSource.load_from_project_binary(project_binary)
        return SimplePersistencyProvider(deds, "demo_athena_project")
    else:
        # Only for local testing
        print("Local testing Persistency Provider")
        return P.PathPersistencyProvider("demo")


provider = create_persistency_provider()
app = create_app(provider)


def handler(event, context):
    base64_content_types = ["image/*", "image/x-icon", "image/png"]
    return awsgi.response(app.server, event, context, base64_content_types)
