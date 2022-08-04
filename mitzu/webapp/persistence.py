import os
import pickle
from typing import List, Optional, Protocol

import mitzu.model as M
import s3fs

PROJECTS_SUB_PATH = "projects"
PROJECT_SUFFIX = ".mitzu"


class PersistencyProvider(Protocol):
    def list_projects(self) -> List[str]:
        pass

    def get_project(self, key: str) -> Optional[M.DiscoveredEventDataSource]:
        pass


class FileSystemPersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str):
        if base_path.endswith("/"):
            base_path = base_path[:-1]
        self.base_path = base_path

    def list_projects(self) -> List[str]:
        res = os.listdir(f"{self.base_path}/{PROJECTS_SUB_PATH}/")
        return [r for r in res if r.endswith(".mitzu")]

    def get_project(self, key: str) -> Optional[M.DiscoveredEventDataSource]:
        if key.endswith(PROJECT_SUFFIX):
            key = key[: len(PROJECT_SUFFIX)]
        path = f"{self.base_path}/{PROJECTS_SUB_PATH}/{key}{PROJECT_SUFFIX}"
        with open(path, "rb") as f:
            return pickle.load(f)


class S3PersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str):
        if base_path.endswith("/"):
            base_path = base_path[:-1]
        self.base_path = base_path
        self.s3fs = s3fs.S3FileSystem(anon=False)

    def list_projects(self) -> List[str]:
        res = self.s3fs.listdir(f"{self.base_path}/{PROJECTS_SUB_PATH}/")
        res = [r["name"].split("/")[-1] for r in res if r["name"].endswith(".mitzu")]
        return res

    def get_project(self, key: str) -> Optional[M.DiscoveredEventDataSource]:
        if key.endswith(PROJECT_SUFFIX):
            key = key[: len(PROJECT_SUFFIX)]
        path = f"{self.base_path}/{PROJECTS_SUB_PATH}/{key}{PROJECT_SUFFIX}"
        with self.s3fs.open(path, "rb") as f:
            return pickle.load(f)
