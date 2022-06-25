from __future__ import annotations

import warnings
from typing import Dict, Optional

import mitzu.model as M


def init_project(
    source: M.EventDataSource,
    config: M.DatasetDiscoveryConfig,
    persist_as: str = None,
    glbs: Optional[Dict] = None,
) -> M.DatasetModel:
    warnings.filterwarnings("ignore")

    dd = source.discover_datasource(config)
    if persist_as is not None:
        dd.save_project(persist_as)

    m = dd.create_notebook_class_model()
    if glbs is not None:
        m._to_globals(glbs)
    return m


def load_project(
    project: str,
    folder: str = "./",
    extension="mitzu",
    glbs: Optional[Dict] = None,
) -> M.DatasetModel:
    warnings.filterwarnings("ignore")

    dd = M.DiscoveredEventDataSource.load_from_project_file(project, folder, extension)
    dd.source.adapter.test_connection()
    m = dd.create_notebook_class_model()
    if glbs is not None:
        m._to_globals(glbs)
    return m
