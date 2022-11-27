from mitzu.model import (
    Connection,
    ConnectionType,
    DiscoveredProject,
    EventDataTable,
    Project,
    PromptSecretResolver,
    ConstSecretResolver,
    EnvVarSecretResolver,
    TimeWindow,
    TimeGroup,
)
from mitzu.samples import get_sample_discovered_project

__all__ = [
    "Connection",
    "ConnectionType",
    "Project",
    "EventDataTable",
    "DiscoveredProject",
    "PromptSecretResolver",
    "ConstSecretResolver",
    "EnvVarSecretResolver",
    "TimeWindow",
    "TimeGroup",
    "get_sample_discovered_project",
]


def load_from_project_file(
    project: str, folder: str = "./", extension="mitzu"
) -> DiscoveredProject:
    return DiscoveredProject.load_from_project_file(project, folder, extension)
