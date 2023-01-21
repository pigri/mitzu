from __future__ import annotations

import base64
import json
import pickle
import mitzu.model as M


DISCOVERED_PROJECT_FILE_VERSION = 2


class DiscoveredProjectSerializationError(Exception):
    pass


def serialize_discovered_project(discovered_project: M.DiscoveredProject) -> str:
    project_binary = pickle.dumps(discovered_project)
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

    res: M.DiscoveredProject = pickle.loads(base64.b64decode(data["project"]))
    res.project._discovered_project.set_value(res)
    for edt in res.project.event_data_tables:
        edt.project.set_value(res.project)

    return res
