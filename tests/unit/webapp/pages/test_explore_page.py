import os
from datetime import datetime
from typing import Any, Dict, Optional, Union

import dash.development.base_component as bc
import mitzu.model as M
import mitzu.webapp.pages.explore.explore_page as EXP
from pytest import fixture
from urllib.parse import unquote


def to_json(input: Any) -> Any:
    if isinstance(input, bc.Component):
        res = input.to_plotly_json()
        if "children" in input.__dict__:
            res["children"] = to_json(input.children)
        if "options" in input.__dict__:
            res["options"] = to_json(input.options)
        if "props" in res:
            res["props"] = to_json(res["props"])
        return res
    if type(input) == dict:
        return {k: to_json(v) for k, v in input.items()}
    if type(input) == list:
        return [to_json(v) for v in input]
    if type(input) == tuple:
        return (to_json(v) for v in input)
    return input


def find_component_by_id(
    comp_id: Union[str, Dict], input: Any
) -> Optional[Dict[str, Any]]:
    if type(input) == list:
        for v in input:
            res = find_component_by_id(comp_id, v)
            if res is not None:
                return res
    elif type(input) == dict:
        if input.get("id") == comp_id:
            return input
        return find_component_by_id(comp_id, list(input.values()))
    return None


@fixture(scope="module")
def discovered_project():
    path = os.path.dirname(os.path.abspath(__file__))
    return M.DiscoveredProject.load_from_project_file(
        project_name="trino_test_project", folder=path
    )


def test_event_chosen_for_segmentation(discovered_project: M.DiscoveredProject):
    all_inputs = {
        "metric_segments": {
            "children": {0: {"children": {0: {"event_name_dropdown": "page_visit"}}}}
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": None,
        "custom_date_picker_end_date": None,
        "lookback_window_dropdown": "1 month",
        "time_window_interval_steps": 5,
        "time_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_json(res[0][0])

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": "event_name_dropdown"}, res
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_name_dropdown"}, res
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, res
    )

    assert second_event_dd is not None
    assert first_property_dd is not None
    assert set([option["value"] for option in first_property_dd["options"]]) == set(
        [
            "page_visit.event_name",
            "page_visit.event_properties.url",
            "page_visit.event_time",
            "page_visit.user_id",
            "page_visit.user_properties.country_code",
            "page_visit.user_properties.is_subscribed",
            "page_visit.user_properties.locale",
        ]
    )
    assert first_property_operator_dd is None


def test_event_property_chosen_for_segmentation(
    discovered_project: M.DiscoveredProject,
):
    all_inputs = {
        "metric_segments": {
            "children": {
                0: {
                    "children": {
                        0: {
                            "event_name_dropdown": "page_visit",
                            "children": {
                                0: {
                                    "property_name_dropdown": "page_visit.user_properties.country_code"
                                }
                            },
                        },
                        1: {"event_name_dropdown": None},
                    },
                    "complex_segment_group_by": None,
                }
            }
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": None,
        "custom_date_picker_end_date": None,
        "lookback_window_dropdown": "1 month",
        "time_window_interval_steps": 5,
        "time_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_json(res[0][0])

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": "event_name_dropdown"}, res
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_name_dropdown"}, res
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, res
    )
    first_property_value_input = find_component_by_id(
        {"index": "0-0-0", "type": "property_value_input"}, res
    )

    assert second_event_dd is not None
    assert first_property_dd is not None
    assert first_property_operator_dd is not None
    assert first_property_value_input is not None
    assert first_property_operator_dd["options"] == [
        "is",
        "is not",
        ">",
        ">=",
        "<",
        "<=",
        "present",
        "missing",
        "like",
        "not like",
    ]
    assert first_property_operator_dd["value"] == "is"
    assert first_property_value_input["options"] == [
        {"label": "br", "value": "br"},
        {"label": "cn", "value": "cn"},
        {"label": "de", "value": "de"},
        {"label": "fr", "value": "fr"},
        {"label": "gb", "value": "gb"},
        {"label": "hu", "value": "hu"},
        {"label": "us", "value": "us"},
    ]
    assert first_property_value_input["value"] == []


def test_event_property_operator_changed_with_values_already_chosen(
    discovered_project: M.DiscoveredProject,
):
    all_inputs = {
        "metric_segments": {
            "children": {
                0: {
                    "children": {
                        0: {
                            "event_name_dropdown": "page_visit",
                            "children": {
                                0: {
                                    "property_operator_dropdown": ">",
                                    "property_name_dropdown": "page_visit.user_properties.is_subscribed",
                                    "property_value_input": [
                                        "organic",
                                        "promo_20off_2020",
                                    ],
                                },
                                1: {"property_name_dropdown": None},
                            },
                        },
                        1: {"event_name_dropdown": None},
                    },
                    "complex_segment_group_by": None,
                }
            }
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker": None,
        "lookback_window_dropdown": "1 month",
        "time_window_interval_steps": 5,
        "time_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_json(res[0][0])

    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, res
    )
    first_property_value_input = find_component_by_id(
        {"index": "0-0-0", "type": "property_value_input"}, res
    )

    assert first_property_operator_dd is not None
    assert first_property_value_input is not None

    assert first_property_operator_dd["value"] == ">"
    assert first_property_value_input["value"] is None


def test_empty_page_with_project(
    discovered_project: M.DiscoveredProject,
):
    all_inputs = {
        "metric_segments": {
            "children": {0: {"children": {0: {"event_name_dropdown": None}}}}
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project?",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": None,
        "custom_date_picker_end_date": None,
        "lookback_window_dropdown": "1 month",
        "time_window_interval_steps": 5,
        "time_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_segs = to_json(res[0])
    metric_confs = to_json(res[1])

    # Metric Segments Part

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": "event_name_dropdown"}, metric_segs
    )
    first_event_dd = find_component_by_id(
        {"index": "0-0", "type": "event_name_dropdown"}, metric_segs
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_name_dropdown"}, metric_segs
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, metric_segs
    )
    first_property_value_input = find_component_by_id(
        {"index": "0-0-0", "type": "property_value_input"}, metric_segs
    )

    assert first_event_dd is not None
    assert first_property_dd is None
    assert second_event_dd is None
    assert first_property_operator_dd is None
    assert first_property_value_input is None

    # Metric Configuration Part

    lookback_dd = find_component_by_id("lookback_window_dropdown", metric_confs)
    timegroup_dd = find_component_by_id("timegroup_dropdown", metric_confs)
    timegroup_dd = find_component_by_id("timegroup_dropdown", metric_confs)

    assert lookback_dd is not None
    assert lookback_dd["value"] == "1 month"
    assert timegroup_dd is not None
    assert timegroup_dd["value"] == M.TimeGroup.DAY.value


def test_custom_date_selected(discovered_project: M.DiscoveredProject):
    all_inputs = {
        "metric_segments": {
            "children": {
                0: {
                    "children": {
                        0: {
                            "event_name_dropdown": "page_visit",
                            "children": {0: {"property_name_dropdown": None}},
                        },
                        1: {"event_name_dropdown": None},
                    },
                    "complex_segment_group_by": None,
                }
            }
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project?",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": None,
        "custom_date_picker_end_date": None,
        "lookback_window_dropdown": "_custom_date",
        "time_window_interval_steps": 5,
        "time_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_confs = to_json(res[1])

    # Metric Segments Part

    date_picker = find_component_by_id("custom_date_picker", metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "inline"}


def test_custom_date_selected_new_start_date(discovered_project: M.DiscoveredProject):
    all_inputs = {
        "metric_segments": {
            "children": {
                0: {
                    "children": {
                        0: {
                            "event_name_dropdown": "page_visit",
                            "children": {0: {"property_name_dropdown": None}},
                        },
                        1: {"event_name_dropdown": None},
                    },
                    "complex_segment_group_by": None,
                }
            }
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project?",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": "2021-12-01T00:00:00",
        "custom_date_picker_end_date": "2022-01-01T00:00:00",
        "lookback_window_dropdown": "_custom_date",
        "time_window_interval_steps": 5,
        "time_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_confs = to_json(res[1])

    # Metric Segments Part

    date_picker = find_component_by_id("custom_date_picker", metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "inline"}
    assert date_picker["start_date"] == datetime(2021, 12, 1, 0, 0)
    assert date_picker["end_date"] == datetime(2022, 1, 1, 0, 0)


def test_custom_date_lookback_days_selected(discovered_project: M.DiscoveredProject):
    all_inputs = {
        "metric_segments": {
            "children": {
                0: {
                    "children": {
                        0: {
                            "event_name_dropdown": "page_visit",
                            "children": {0: {"property_name_dropdown": None}},
                        },
                        1: {"event_name_dropdown": None},
                    },
                    "complex_segment_group_by": None,
                }
            }
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project",
        "metric-type-dropdown": "segmentation",
        "event_name_dropdown": ["page_visit", None],
        "property_operator_dropdown": [],
        "property_name_dropdown": [None],
        "property_value_input": [],
        "complex_segment_group_by": [None],
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": "2021-12-01T00:00:00",
        "custom_date_picker_end_date": "2022-01-01T00:00:00",
        "lookback_window_dropdown": "1 month",
        "time_window_interval_steps": 5,
        "time_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_confs = to_json(res[1])

    # Metric Segments Part

    date_picker = find_component_by_id("custom_date_picker", metric_confs)
    lookback_days_dd = find_component_by_id("lookback_window_dropdown", metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "none"}
    assert date_picker["start_date"] is None
    assert date_picker["end_date"] is None

    assert lookback_days_dd is not None
    assert lookback_days_dd["value"] == "1 month"


def test_mitzu_link_redirected(discovered_project: M.DiscoveredProject):
    query_params = {
        "m": unquote(
            "eNolTEsKgCAUvIrMulXLLiOm"
            "DxNSI59BiHfPZ5v5MJ%2BGQh6bajgnUhqEy3jSTyiB0fuiYPNf2Z2kq4o58YERsGzhzCvGshhRZqpa6NY21yQvH0z5HoY%3D"
        )
    }
    res = EXP.create_explore_page(query_params, discovered_project)

    explore_page = to_json(res)

    # Metric Segments Part

    first_event_dd = find_component_by_id(
        {"index": "0-0", "type": "event_name_dropdown"}, explore_page
    )

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": "event_name_dropdown"}, explore_page
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_name_dropdown"}, explore_page
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, explore_page
    )

    assert first_event_dd is not None
    assert first_event_dd["value"] == "page_visit"
    assert second_event_dd is not None
    assert first_property_dd is not None
    assert set([option["value"] for option in first_property_dd["options"]]) == set(
        [
            "page_visit.event_name",
            "page_visit.event_properties.url",
            "page_visit.event_time",
            "page_visit.user_id",
            "page_visit.user_properties.country_code",
            "page_visit.user_properties.is_subscribed",
            "page_visit.user_properties.locale",
        ]
    )
    assert first_property_operator_dd is None

    # Metric confs Part

    date_picker = find_component_by_id("custom_date_picker", explore_page)
    lookback_days_dd = find_component_by_id("lookback_window_dropdown", explore_page)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "none"}
    assert date_picker["start_date"] is None
    assert date_picker["end_date"] is None
    assert lookback_days_dd is not None
    assert lookback_days_dd["value"] == "2 months"


def test_event_chosen_for_retention(discovered_project: M.DiscoveredProject):
    all_inputs = {
        "metric_segments": {
            "children": {
                0: {"children": {0: {"event_name_dropdown": "page_visit"}}},
                1: {"children": {0: {"event_name_dropdown": "checkout"}}},
            }
        },
        "metric-type-dropdown": "retention",
        "timegroup_dropdown": 1,
        "custom_date_picker_start_date": None,
        "custom_date_picker_end_date": None,
        "lookback_window_dropdown": "1 month",
        "time_window_interval_steps": 1,
        "time_window_interval": 1,
        "aggregation_type": "retention_rate",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_json(res[0][0])

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": "event_name_dropdown"}, res
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_name_dropdown"}, res
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, res
    )

    assert second_event_dd is not None
    assert first_property_dd is not None
    assert len(first_property_dd["options"]) == 7
    assert first_property_operator_dd is None