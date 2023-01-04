from __future__ import annotations

import mitzu.model as M
import mitzu.samples.data_ingestion as DI
from typing import Optional


def get_sample_discovered_project(
    random_seed: Optional[int] = None,
) -> M.DiscoveredProject:
    """Generates a random discovered project, ready to be used.

    Args:
        random_seed (Optional[int], optional): the seed value for the random event generator. If None it defaults to a new seed every time.

    Returns:
        M.DiscoveredProject: The discovered project that is ready to be used.
    """
    connection = M.Connection(
        connection_type=M.ConnectionType.SQLITE, connection_name="Sample project"
    )

    project = DI.create_and_ingest_sample_project(connection, seed=random_seed)
    return project.discover_project()
