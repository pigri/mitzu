from __future__ import annotations

import mitzu.model as M
import mitzu.samples.data_ingestion as DI


def get_simple_discovered_project() -> M.DiscoveredProject:
    connection = M.Connection(connection_type=M.ConnectionType.SQLITE)

    project = DI.create_and_ingest_sample_project(connection)
    return project.discover_project()
