import pytest
from unittest.mock import MagicMock
from datetime import datetime

from mitzu.model import (
    DiscoverySettings,
    EventDataTable,
    InvalidProjectError,
    Project,
    Segment,
)
from mitzu.project_discovery import ProjectDiscovery, ProjectDiscoveryError
from tests.helper import assert_row
from tests.samples.sources import (
    get_project_with_missing_table,
    get_project_without_records,
    get_simple_big_data,
    get_simple_csv,
)


def test_simple_big_data_discovery():
    project = get_simple_big_data()

    callback = MagicMock()
    discovery = ProjectDiscovery(project, callback=callback)
    m = discovery.discover_project().create_notebook_class_model()

    callback.assert_called_once()
    assert isinstance(callback.call_args[0][0], EventDataTable)
    assert set(callback.call_args[0][1].keys()) == set(
        [
            "app_install",
            "app_launched",
            "new_subscription",
            "trial_started",
            "user_signed_up",
            "workspace_opened",
        ]
    )
    assert callback.call_args[0][2] is None

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01",
        end_dt="2022-01-01",
        time_group="total",
    )
    df = seg.get_df()
    assert 1 == df.shape[0]
    assert_row(df, _agg_value=2254, _datetime=None)

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01",
        end_dt="2022-01-01",
        time_group="total",
        aggregation="event_count",
    )
    df = seg.get_df()
    assert 1 == df.shape[0]
    assert_row(df, _agg_value=4706, _datetime=None)


def test_data_discovery_without_data():
    project = get_project_without_records()
    callback = MagicMock()
    discovery = ProjectDiscovery(project, callback=callback)

    dp = discovery.discover_project()
    assert len(dp.get_all_events()) == 0
    callback.assert_called_once()
    assert isinstance(callback.call_args[0][0], EventDataTable)
    assert callback.call_args[0][1] == {}
    assert isinstance(callback.call_args[0][2], ProjectDiscoveryError)


def test_data_discovery_with_missing_table():
    project = get_project_with_missing_table()
    discovery = ProjectDiscovery(project)
    with pytest.raises(InvalidProjectError) as e_info:
        discovery.discover_project()

    assert "missing.csv" in str(e_info.value)


def test_event_data_table_discovery_settings_used():
    project_config = get_simple_csv().__dict__
    project_config.update(
        discovery_settings=DiscoverySettings(end_dt=datetime(2010, 1, 1))
    )
    for key in [key for key in project_config.keys() if key.startswith("_")]:
        del project_config[key]

    del project_config["id"]
    project = Project(**project_config)

    dp = ProjectDiscovery(project).discover_project()
    assert len(dp.get_all_events()) == 0

    new_edt = project.event_data_tables[0].update_discovery_settings(
        DiscoverySettings(
            max_enum_cardinality=300,
            max_map_key_cardinality=300,
            end_dt=datetime(2022, 1, 1),
            lookback_days=2000,
        )
    )
    project_config.update(event_data_tables=[new_edt])
    project = Project(**project_config)

    dp = ProjectDiscovery(project).discover_project()
    assert len(dp.get_all_events()) == 0
