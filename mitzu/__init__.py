import mitzu.project as P
from mitzu.common.model import EventDataSource, Connection, ConnectionType  # type: ignore
from datetime import datetime
from mitzu.notebook.model_loader import DatasetModel
import inspect
from typing import Dict, Optional

__all__ = ["init_project", "Connection", "ConnectionType"]


def find_notebook_globals() -> Optional[Dict]:
    for stk in inspect.stack():
        parent_globals = stk[0].f_globals
        if "init_notebook_project" in parent_globals:
            return parent_globals
    return None


def init_notebook_project(
    source: EventDataSource,
    start_dt: datetime = None,
    end_dt: datetime = None,
    glbs=None,
) -> DatasetModel:
    if glbs is None:
        glbs = find_notebook_globals()
    return P.init_project(source=source, start_dt=start_dt, end_dt=end_dt, glbs=glbs)
