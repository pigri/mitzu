from __future__ import annotations

import os
import pickle
from abc import ABC
from pathlib import Path
from typing import List, Optional

import mitzu.model as M
import s3fs
from mitzu.samples.data_ingestion import create_and_ingest_sample_project

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
            res: M.DiscoveredProject = pickle.load(f)
            res.project._discovered_project.set_value(res)
            return res


class S3PersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str, projects_path: str = PROJECTS_SUB_PATH):
        super().__init__()
        if base_path.endswith("/"):
            base_path = base_path[:-1]
        self.base_path = base_path
        self.projects_path = projects_path
        self.s3fs = s3fs.S3FileSystem(anon=False)

    def list_projects(self) -> List[str]:
        res = self.s3fs.listdir(f"{self.base_path}/{self.projects_path}/")
        res = [r["name"].split("/")[-1] for r in res if r["name"].endswith(".mitzu")]
        if len(res) == 0:
            return super().list_projects()
        return res

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        if key.endswith(PROJECT_SUFFIX):
            key = key[: len(PROJECT_SUFFIX)]
        if key == SAMPLE_PROJECT_NAME:
            return super().get_project(key)
        path = f"{self.base_path}/{self.projects_path}/{key}{PROJECT_SUFFIX}"

        with self.s3fs.open(path, "rb") as f:
            res: M.DiscoveredProject = pickle.load(f)
            res.project._discovered_project.set_value(res)
            return res
