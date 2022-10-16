import pytest

from mitzu.model import Segment
from mitzu.project_discovery import ProjectDiscovery, ProjectDiscoveryError
from mitzu.model import InvalidProjectError
from tests.helper import assert_row
from tests.samples.sources import (
    get_simple_big_data,
    get_project_without_records,
    get_project_with_missing_table,
)


def test_simple_big_data_discovery():
    project = get_simple_big_data()
    discovery = ProjectDiscovery(project)
    m = discovery.discover_project().create_notebook_class_model()

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01",
        end_dt="2022-01-01",
        time_group="total",
    )
    df = seg.get_df()
    print(df)
    assert 1 == df.shape[0]
    assert_row(df, _agg_value=2254, _datetime=None)

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01",
        end_dt="2022-01-01",
        time_group="total",
        aggregation="event_count",
    )
    df = seg.get_df()
    print(df)
    assert 1 == df.shape[0]
    assert_row(df, _agg_value=4706, _datetime=None)


def test_data_discovery_without_data():
    project = get_project_without_records()
    discovery = ProjectDiscovery(project)
    with pytest.raises(ProjectDiscoveryError) as e_info:
        discovery.discover_project()

    assert "'simple'" in str(e_info.value)


def test_data_discovery_with_missing_table():
    project = get_project_with_missing_table()
    discovery = ProjectDiscovery(project)
    with pytest.raises(InvalidProjectError) as e_info:
        discovery.discover_project()

    assert "missing.csv" in str(e_info.value)
