from __future__ import annotations

import os
from abc import ABC
from pathlib import Path
from typing import List, Optional, Tuple

import mitzu.model as M
from mitzu.samples.data_ingestion import create_and_ingest_sample_project
import boto3
from urllib import parse

PROJECTS_SUB_PATH = "projects"
PROJECT_SUFFIX = ".mitzu"
SAMPLE_PROJECT_NAME = "sample_project"


def create_sample_project() -> M.DiscoveredProject:
    connection = M.Connection(
        connection_type=M.ConnectionType.SQLITE,
    )
    project = create_and_ingest_sample_project(
        connection, event_count=200000, number_of_users=1000
    )
    return project.discover_project()


class PersistencyProvider(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.sample_project: Optional[M.DiscoveredProject] = None

    def list_projects(self) -> List[str]:
        return [SAMPLE_PROJECT_NAME]

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        if key == SAMPLE_PROJECT_NAME:
            if self.sample_project is None:
                self.sample_project = create_sample_project()
            return self.sample_project
        else:
            return None


class FileSystemPersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str = "./", projects_path: str = PROJECTS_SUB_PATH):
        super().__init__()
        if base_path.endswith("/"):
            base_path = base_path[:-1]
        self.base_path = base_path
        self.projects_path = projects_path

    def list_projects(self) -> List[str]:
        folder = Path(f"{self.base_path}/{self.projects_path}/")
        folder.mkdir(parents=True, exist_ok=True)
        res = os.listdir(folder)
        res = [r for r in res if r.endswith(".mitzu")]

        if len(res) == 0:
            return super().list_projects()
        return res

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        if key.endswith(PROJECT_SUFFIX):
            key = key[: len(PROJECT_SUFFIX)]
        if key == SAMPLE_PROJECT_NAME:
            return super().get_project(key)
        folder = Path(f"{self.base_path}/{self.projects_path}/")
        folder.mkdir(parents=True, exist_ok=True)
        path = f"{folder}/{key}{PROJECT_SUFFIX}"
        with open(path, "rb") as f:
            res = M.DiscoveredProject.deserialize(f.read())
            res.project._discovered_project.set_value(res)
            return res


class S3PersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str, projects_path: str = PROJECTS_SUB_PATH):
        super().__init__()
        if base_path.endswith("/"):
            base_path = base_path[:-1]
        self.base_path = base_path
        self.projects_path = projects_path

    def get_project_bucket_and_path(self) -> Tuple[str, str]:
        s3_url = parse.urlparse(self.base_path)
        paths = s3_url.path.split("/")
        bucket = paths[0]
        path = "/".join(paths[1:] + [self.projects_path]) + "/"
        return bucket, path

    def list_projects(self) -> List[str]:

        bucket, prefix = self.get_project_bucket_and_path()
        conn = boto3.client("s3")
        res = [
            key["Key"]
            for key in conn.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]
        ]

        res = [r.split("/")[-1] for r in res if r.endswith(".mitzu")]
        if len(res) == 0:
            return super().list_projects()
        return res

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        if key.endswith(PROJECT_SUFFIX):
            key = key[: len(PROJECT_SUFFIX)]
        if key == SAMPLE_PROJECT_NAME:
            return super().get_project(key)

        bucket, prefix = self.get_project_bucket_and_path()
        conn = boto3.client("s3")

        full_key = prefix + key
        if not full_key.endswith(PROJECT_SUFFIX):
            full_key = full_key + PROJECT_SUFFIX

        obj = conn.get_object(Bucket=bucket, Key=full_key)
        raw_discovered_project = obj["Body"].read()
        res = M.DiscoveredProject.deserialize(raw_discovered_project)

        res.project._discovered_project.set_value(res)
        return res
