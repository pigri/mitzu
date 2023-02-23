from __future__ import annotations

import base64
import json
import pickle
import mitzu.model as M
from typing import Dict, Any


DISCOVERED_PROJECT_FILE_VERSION = 3

DEFS = "defs"
PROJECT = "project"
CONNECTION = "connection"


class DiscoveredProjectSerializationError(Exception):
    pass


def serialize_discovered_project(dp: M.DiscoveredProject) -> str:
    definitions = {}
    for edt, defs in dp.definitions.items():
        edt_def = {}
        for evt_name, evt_def in defs.items():
            edt_def[evt_name] = evt_def.get_value_if_exists()
        definitions[edt] = edt_def

    serializable = {
        DEFS: definitions,
        PROJECT: dp.project,
        CONNECTION: dp.project.connection,
    }

    project_binary = pickle.dumps(serializable)
    data = {
        "version": DISCOVERED_PROJECT_FILE_VERSION,
        "project": base64.b64encode(project_binary).decode("UTF-8"),
    }
    return json.dumps(data)


def deserialize_discovered_project(raw_data: bytes) -> M.DiscoveredProject:
    try:
        data = json.loads(raw_data)
        if data["version"] != DISCOVERED_PROJECT_FILE_VERSION:
            raise DiscoveredProjectSerializationError(
                "Invalid discovered project version. Please discover the project again."
            )
    except Exception as e:
        raise DiscoveredProjectSerializationError(
            "Something went wrong, cannot deserialize discovered project file.\n"
            "Try discovering the project again."
        ) from e

    res: Dict[str, Any] = pickle.loads(base64.b64decode(data["project"]))

    project: M.Project = res[PROJECT]

    for edt in project.event_data_tables:
        edt.set_project(project)
    project.set_connection(res[CONNECTION])

    definitions = {}
    for edt, defs in res[DEFS].items():
        edt_def: Dict[str, M.Reference[M.EventDef]] = {}
        for evt_name, evt_def in defs.items():
            edt_def[evt_name] = M.Reference(evt_def)
        definitions[edt] = edt_def

    return M.DiscoveredProject(definitions=definitions, project=project)
