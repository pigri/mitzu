import tempfile
from datetime import datetime
from pathlib import Path

import mitzu.model as M
from mitzu.samples.test_data_generator import generate_web_events


def get_simple_discovered_project() -> M.DiscoveredProject:
    df = generate_web_events()
    tempdir = tempfile.gettempdir()

    df.to_parquet(Path(tempdir, "web_events.parquet"))

    project = M.Project(
        event_data_tables=[
            M.EventDataTable.create(
                table_name="web_events",
                event_time_field="event_time",
                user_id_field="user_id",
                event_name_field="event_name",
            )
        ],
        default_discovery_lookback_days=100,
        default_end_dt=datetime(2022, 10, 1),
        connection=M.Connection(
            M.ConnectionType.FILE,
            extra_configs={
                "file_type": "parquet",
                "path": tempdir,
            },
        ),
    )

    return project.discover_project()
