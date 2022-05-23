import inspect
from datetime import datetime
from typing import Dict, Optional

import mitzu.project as P
from mitzu.common.model import (
    Connection,
    ConnectionType,
    DatasetModel,
    EventDataSource,
    EventDataTable,
)

Connection
ConnectionType
EventDataSource
EventDataTable


def _find_notebook_globals() -> Optional[Dict]:
    for stk in inspect.stack():
        parent_globals = stk[0].f_globals
        if (
            "init_project" in parent_globals or "load_project"
        ) and parent_globals != globals():
            print("Found notebook context")
            return parent_globals
    print("Couldn't find notebook context")
    return None


def load_project(project: str, folder: str = "./", extension="mitzu", glbs=None):
    if glbs is None:
        glbs = _find_notebook_globals()
    print("Initializing project ...")
    res = P.load_project(project, folder, extension, glbs=glbs)
    print("Finished project initialization")
    return res


def init_project(
    source: EventDataSource,
    start_date: datetime = None,
    end_date: datetime = None,
    persist_as: str = None,
    glbs=None,
) -> DatasetModel:
    if glbs is None:
        glbs = _find_notebook_globals()
    print("Initializing project ...")
    res = P.init_project(
        source=source,
        start_date=start_date,
        end_date=end_date,
        glbs=glbs,
        persist_as=persist_as,
    )
    print("Finished project initialization")
    return res
