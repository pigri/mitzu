from mitzu.model import Metric, Project
from mitzu.project_discovery import ProjectDiscovery
from mitzu.serialization import (
    from_compressed_string,
    from_dict,
    to_compressed_string,
    to_dict,
)
from tests.samples.sources import get_simple_csv


def verify(metric: Metric, project: Project):
    metric_json = to_dict(metric)
    metric2 = from_dict(metric_json, project)
    compared_df = metric2.get_df().compare(metric.get_df(), align_axis=0)
    assert compared_df.shape[0] == 0


def test_definition_to_json():
    eds = get_simple_csv()
    discovery = ProjectDiscovery(eds)
    dd1 = discovery.discover_project()
    m = dd1.create_notebook_class_model()

    # Test Segmentation
    res = (m.view.category_id.is_not_null | m.cart).config(
        start_dt="2020-01-01",
        end_dt="2021-01-01",
        time_group="total",
        group_by=m.view.category_id,
    )

    res_dict = to_dict(res)
    assert res_dict == {
        "seg": {
            "l": {"l": {"en": "view", "f": "category_id"}, "op": "IS_NOT_NULL"},
            "bop": "OR",
            "r": {"l": {"en": "cart"}},
        },
        "co": {
            "sdt": "2020-01-01T00:00:00",
            "edt": "2021-01-01T00:00:00",
            "tg": "total",
            "gb": {"en": "view", "f": "category_id"},
        },
    }
    verify(res, eds)

    # Test Conversion
    res = (m.view.category_id.is_not_null >> m.cart).config(
        start_dt="2020-01-01",
        end_dt="2021-01-01",
        time_group="week",
        conv_window="1 week",
        group_by=m.view.category_id,
        custom_title="test_title",
    )

    res_dict = to_dict(res)
    assert res_dict == {
        "conv": {
            "segs": [
                {"l": {"en": "view", "f": "category_id"}, "op": "IS_NOT_NULL"},
                {"l": {"en": "cart"}},
            ]
        },
        "cw": "1 week",
        "co": {
            "sdt": "2020-01-01T00:00:00",
            "edt": "2021-01-01T00:00:00",
            "tg": "week",
            "gb": {"en": "view", "f": "category_id"},
            "ct": "test_title",
        },
    }

    verify(res, eds)

    # Test Simple Definitions
    verify(m.view, eds)
    verify(m.view | m.cart, eds)
    verify(m.view >> m.cart, eds)
    verify(m.view.config(start_dt="2021-01-01", end_dt="2021-10-01"), eds)
    verify((m.view >> m.cart).config(start_dt="2021-01-01", end_dt="2021-10-01"), eds)


def test_compression():
    eds = get_simple_csv()
    discovery = ProjectDiscovery(eds)
    dd1 = discovery.discover_project()
    m = dd1.create_notebook_class_model()

    # Test Segmentation
    res = (m.view.category_id.is_not_null | m.cart).config(
        start_dt="2020-01-01",
        end_dt="2021-01-01",
        time_group="total",
        group_by=m.view.category_id,
    )

    compressed = to_compressed_string(res)

    re_compress = to_compressed_string(from_compressed_string(compressed, eds))

    assert compressed == re_compress
