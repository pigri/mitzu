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

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        pass


class FileSystemPersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str = "./", projects_path: str = PROJECTS_SUB_PATH):
        if base_path.endswith("/"):
            base_path = base_path[:-1]
        self.base_path = base_path
        self.projects_path = projects_path

    def list_projects(self) -> List[str]:
        res = os.listdir(f"{self.base_path}/{self.projects_path}/")
        return [r for r in res if r.endswith(".mitzu")]

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        if key.endswith(PROJECT_SUFFIX):
            key = key[: len(PROJECT_SUFFIX)]
        path = f"{self.base_path}/{self.projects_path}/{key}{PROJECT_SUFFIX}"
        with open(path, "rb") as f:
            res: M.DiscoveredProject = pickle.load(f)
            res.project._discovered_project.set_value(res)
            return res


class S3PersistencyProvider(PersistencyProvider):
    def __init__(self, base_path: str, projects_path: str = PROJECTS_SUB_PATH):
        if base_path.endswith("/"):
            base_path = base_path[:-1]
        self.base_path = base_path
        self.projects_path = projects_path
        self.s3fs = s3fs.S3FileSystem(anon=False)

    def list_projects(self) -> List[str]:
        res = self.s3fs.listdir(f"{self.base_path}/{self.projects_path}/")
        res = [r["name"].split("/")[-1] for r in res if r["name"].endswith(".mitzu")]
        return res

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        if key.endswith(PROJECT_SUFFIX):
            key = key[: len(PROJECT_SUFFIX)]
        path = f"{self.base_path}/{self.projects_path}/{key}{PROJECT_SUFFIX}"
        with self.s3fs.open(path, "rb") as f:
            res: M.DiscoveredProject = pickle.load(f)
            res.project._discovered_project.set_value(res)
            return res
        return res
