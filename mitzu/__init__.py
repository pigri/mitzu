import inspect
from typing import Dict

from mitzu.model import (
    Connection,
    ConnectionType,
    DiscoveredProject,
    EventDataTable,
    Project,
)

Connection
ConnectionType
Project
EventDataTable
DiscoveredProject


def _find_notebook_globals() -> Dict:
    for stk in inspect.stack():
        glbs = stk[0].f_globals
        if ("load_from_project_file" in glbs) and "_find_notebook_globals" not in glbs:
            return glbs
    print("Warn: Parent globals not found")
    return {}


def load_from_project_file(
    project: str, folder: str = "./", extension="mitzu", set_globals: bool = True
):
    print("Initializing project ...")
    m = DiscoveredProject.load_from_project_file(
        project, folder, extension
    ).create_notebook_class_model()

    if set_globals:
        glbs = _find_notebook_globals()
        m._to_globals(glbs)
        print("Globals set up")

    print("Finished project initialization")
    return m
