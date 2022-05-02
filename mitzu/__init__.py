import mitzu.project as P
from mitzu.common.model import EventDataSource, Connection, ConnectionType, EventDataTable  # type: ignore
from datetime import datetime
from mitzu.notebook.model_loader import DatasetModel
import inspect
from typing import Dict, Optional

__all__ = [
    "init_notebook_project",
    "Connection",
    "ConnectionType",
    "EventDataSource",
    "EventDataTable",
]


Connection
ConnectionType
EventDataSource


def _find_notebook_globals() -> Optional[Dict]:
    for stk in inspect.stack():
        parent_globals = stk[0].f_globals
        if "init_notebook_project" in parent_globals and parent_globals != globals():
            print("Found notebook context")
            return parent_globals
    print("Couldn't find notebook context")
    return None


def init_notebook_project(
    source: EventDataSource,
    start_dt: datetime = None,
    end_dt: datetime = None,
    glbs=None,
) -> DatasetModel:
    if glbs is None:
        glbs = _find_notebook_globals()
    print("Initializing project ...")
    res = P.init_project(source=source, start_dt=start_dt, end_dt=end_dt, glbs=glbs)
    print("Finished project initialization")
    return res
