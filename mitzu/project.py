from __future__ import annotations

import warnings
from datetime import datetime
from typing import Dict, Optional

import mitzu.common.model as M


def init_project(
    source: M.EventDataSource,
    persist_as: str = None,
    start_date: datetime = None,
    end_date: datetime = None,
    glbs: Optional[Dict] = None,
) -> M.DatasetModel:
    warnings.filterwarnings("ignore")

    dd = source.discover_datasource(start_date=start_date, end_date=end_date)
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
