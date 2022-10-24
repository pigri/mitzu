import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import dash.development.base_component as bc
import mitzu.model as M
from dash import Dash
from mitzu.webapp.persistence import PersistencyProvider
from mitzu.webapp.webapp import MitzuWebApp


class DummyPersistencyProvider(PersistencyProvider):
    def list_projects(self) -> List[str]:
        return []

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        return None


CURR_DIR = os.path.dirname(os.path.abspath(__file__))

MWA = MitzuWebApp(
    app=Dash(__name__),
    authorizer=None,
    persistency_provider=DummyPersistencyProvider(),
    discovered_project_cache={
        "trino_test_project": M.DiscoveredProject.load_from_project_file(
            project_name="trino_test_project", folder=CURR_DIR
        )
    },
)


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


def find_component_by_id(comp_id: str, input: Any) -> Optional[Dict[str, Any]]:
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


def test_event_chosen_for_segmentation():
    ctx_triggered_id = {"index": "0-0", "type": "event_name_dropdown"}
    all_inputs = {
        "metric_segments": {
            "children": {0: {"children": {0: {"event_name_dropdown": "page_visit"}}}}
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": None,
        "custom_date_picker_end_date": None,
        "lookback_window_dropdown": 30,
        "conversion_window_interval_steps": 5,
        "conversion_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
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
    assert len(first_property_dd["options"]) == 8
    assert first_property_operator_dd is None


def test_event_property_chosen_for_segmentation():
    ctx_triggered_id = {"index": "0-0-0", "type": "property_name_dropdown"}
    all_inputs = {
        "metric_segments": {
            "children": {
                0: {
                    "children": {
                        0: {
                            "event_name_dropdown": "page_visit",
                            "children": {
                                0: {
                                    "property_name_dropdown": "page_visit.user_properties.aquisition_campaign"
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
        "lookback_window_dropdown": 30,
        "conversion_window_interval_steps": 5,
        "conversion_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
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
        {"label": "christmass_2020", "value": "christmass_2020"},
        {"label": "organic", "value": "organic"},
        {"label": "promo_20off_2020", "value": "promo_20off_2020"},
        {"label": "spring_sale_2020", "value": "spring_sale_2020"},
        {"label": "summer_sale_2020", "value": "summer_sale_2020"},
    ]
    assert first_property_value_input["value"] == []


def test_event_property_operator_changed_with_values_already_chosen():
    ctx_triggered_id = {"index": "0-0-0", "type": "property_operator_dropdown"}
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
                                    "property_name_dropdown": "page_visit.user_properties.aquisition_campaign",
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
        "lookback_window_dropdown": 30,
        "conversion_window_interval_steps": 5,
        "conversion_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
    res = to_json(res[0][0])

    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, res
    )
    first_property_value_input = find_component_by_id(
        {"index": "0-0-0", "type": "property_value_input"}, res
    )

    assert first_property_operator_dd["value"] == ">"
    assert first_property_value_input["value"] is None


def test_empty_page_with_project():
    ctx_triggered_id = "mitzu_location"
    all_inputs = {
        "metric_segments": {
            "children": {0: {"children": {0: {"event_name_dropdown": None}}}}
        },
        "mitzu_location": "http://127.0.0.1:8082/trino_test_project?",
        "metric-type-dropdown": "segmentation",
        "timegroup_dropdown": 5,
        "custom_date_picker_start_date": None,
        "custom_date_picker_end_date": None,
        "lookback_window_dropdown": 30,
        "conversion_window_interval_steps": 5,
        "conversion_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
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
    assert lookback_dd["value"] == 30
    assert timegroup_dd is not None
    assert timegroup_dd["value"] == M.TimeGroup.DAY.value


def test_custom_date_selected():
    ctx_triggered_id = "lookback_window_dropdown"
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
        "lookback_window_dropdown": -1,
        "conversion_window_interval_steps": 5,
        "conversion_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
    metric_confs = to_json(res[1])

    # Metric Segments Part

    date_picker = find_component_by_id("custom_date_picker", metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "inline"}


def test_custom_date_selected_new_start_date():
    ctx_triggered_id = "custom_date_picker"
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
        "lookback_window_dropdown": -1,
        "conversion_window_interval_steps": 5,
        "conversion_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
    metric_confs = to_json(res[1])

    # Metric Segments Part

    date_picker = find_component_by_id("custom_date_picker", metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "inline"}
    assert date_picker["start_date"] == datetime(2021, 12, 1, 0, 0)
    assert date_picker["end_date"] == datetime(2022, 1, 1, 0, 0)


def test_custom_date_lookback_days_selected():
    ctx_triggered_id = "custom_date_picker"
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
        "lookback_window_dropdown": 30,
        "conversion_window_interval_steps": 5,
        "conversion_window_interval": 1,
        "aggregation_type": "user_count",
        "graph_refresh_button": None,
        "chart_button": None,
        "table_button": None,
        "sql_button": None,
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
    metric_confs = to_json(res[1])

    # Metric Segments Part

    date_picker = find_component_by_id("custom_date_picker", metric_confs)
    lookback_days_dd = find_component_by_id("lookback_window_dropdown", metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "none"}
    assert date_picker["start_date"] is None
    assert date_picker["end_date"] is None

    assert lookback_days_dd["value"] == 30


def test_mitzu_link_redirected():
    ctx_triggered_id = "mitzu_location"
    all_inputs = {
        "mitzu_location": (
            "http://localhost:8082/trino_test_project?m=eNqrVipOTVeyUqhWygGTqXlASqkgM"
            "T01viyzOLNEqbZWR0EpOR%2BiJCkFJGtsoJCSWKkEFC8BaVWCcpJLQBwQKxHMKi1OLYpPzi/NAxkCABkvHc8="
        )
    }

    res = MWA.handle_input_changes(all_inputs, ctx_triggered_id)
    metric_segs = to_json(res[0])

    # Metric Segments Part

    first_event_dd = find_component_by_id(
        {"index": "0-0", "type": "event_name_dropdown"}, metric_segs
    )

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": "event_name_dropdown"}, metric_segs
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_name_dropdown"}, metric_segs
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": "property_operator_dropdown"}, metric_segs
    )

    assert first_event_dd is not None
    assert first_event_dd["value"] == "page_visit"
    assert second_event_dd is not None
    assert first_property_dd is not None
    assert len(first_property_dd["options"]) == 8
    assert first_property_operator_dd is None

    metric_confs = to_json(res[1])

    # Metric confs Part

    date_picker = find_component_by_id("custom_date_picker", metric_confs)
    lookback_days_dd = find_component_by_id("lookback_window_dropdown", metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"display": "none"}
    assert date_picker["start_date"] is None
    assert date_picker["end_date"] is None

    assert lookback_days_dd["value"] == 30
