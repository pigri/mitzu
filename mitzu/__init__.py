from mitzu.model import (
    Connection,
    ConnectionType,
    DiscoveredProject,
    EventDataTable,
    Project,
)
import mitzu.helper as helper

Connection
ConnectionType
Project
EventDataTable
DiscoveredProject


def load_from_project_file(
    project: str, folder: str = "./", extension="mitzu", set_globals: bool = True
):
    m = DiscoveredProject.load_from_project_file(
        project, folder, extension
    ).create_notebook_class_model()

    if set_globals:
        glbs = helper.find_notebook_globals("load_from_project_file")
        m._to_globals(glbs)

    return m
