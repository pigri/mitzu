from tests.helper import to_dict, find_component_by_id
import mitzu.model as M
import mitzu.webapp.pages.explore.metric_segments_handler as MS
import mitzu.webapp.pages.explore.metric_config_handler as MC
import mitzu.webapp.pages.explore.simple_segment_handler as SS
import mitzu.webapp.pages.explore.complex_segment_handler as CS
import mitzu.webapp.pages.explore.event_segment_handler as ES
import mitzu.webapp.pages.explore.dates_selector_handler as DS
import mitzu.webapp.pages.explore.toolbar_handler as TH
import mitzu.webapp.pages.explore.explore_page as EXP
import mitzu.webapp.helper as H
import mitzu.webapp.pages.explore.metric_type_handler as MTH
import mitzu.webapp.dependencies as DEPS
from urllib.parse import unquote

from datetime import datetime


def test_event_chosen_for_segmentation(discovered_project: M.DiscoveredProject):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {0: {"children": {0: {ES.EVENT_NAME_DROPDOWN: "page_visit"}}}}
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "segmentation",
        DS.TIME_GROUP_DROPDOWN: 5,
        DS.CUSTOM_DATE_PICKER: [None, None],
        DS.LOOKBACK_WINDOW_DROPDOWN: "1 month",
        MC.TIME_WINDOW_INTERVAL_STEPS: 5,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "user_count",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        TH.GRAPH_CONTENT_TYPE: TH.CHART_VAL,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_dict(res[MS.METRIC_SEGMENTS][0])

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": ES.EVENT_NAME_DROPDOWN}, res
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_NAME_DROPDOWN}, res
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_OPERATOR_DROPDOWN}, res
    )

    assert second_event_dd is not None
    assert first_property_dd is not None
    assert set([option["value"] for option in first_property_dd["options"]]) == set(
        [
            "page_visit.acquisition_campaign",
            "page_visit.domain",
            "page_visit.event_name",
            "page_visit.event_time",
            "page_visit.item_id",
            "page_visit.title",
            "page_visit.user_country_code",
            "page_visit.user_id",
            "page_visit.user_locale",
        ]
    )
    assert first_property_operator_dd is None


def test_event_property_chosen_for_segmentation(
    discovered_project: M.DiscoveredProject,
):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {
                0: {
                    "children": {
                        0: {
                            ES.EVENT_NAME_DROPDOWN: "page_visit",
                            "children": {
                                0: {
                                    SS.PROPERTY_NAME_DROPDOWN: "page_visit.acquisition_campaign"
                                }
                            },
                        },
                        1: {ES.EVENT_NAME_DROPDOWN: None},
                    },
                    CS.COMPLEX_SEGMENT_GROUP_BY: None,
                }
            }
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "segmentation",
        DS.TIME_GROUP_DROPDOWN: 5,
        DS.CUSTOM_DATE_PICKER: [None, None],
        DS.LOOKBACK_WINDOW_DROPDOWN: "1 month",
        MC.TIME_WINDOW_INTERVAL_STEPS: 5,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "user_count",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_dict(res[MS.METRIC_SEGMENTS][0])

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": ES.EVENT_NAME_DROPDOWN}, res
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_NAME_DROPDOWN}, res
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_OPERATOR_DROPDOWN}, res
    )
    first_property_value_input = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_VALUE_INPUT}, res
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
    assert len(first_property_value_input["options"]) > 1
    assert first_property_value_input["value"] == []


def test_event_property_operator_changed_with_values_already_chosen(
    discovered_project: M.DiscoveredProject,
):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {
                0: {
                    "children": {
                        0: {
                            ES.EVENT_NAME_DROPDOWN: "page_visit",
                            "children": {
                                0: {
                                    SS.PROPERTY_OPERATOR_DROPDOWN: ">",
                                    SS.PROPERTY_NAME_DROPDOWN: "page_visit.acquisition_campaign",
                                    SS.PROPERTY_VALUE_INPUT: [
                                        "organic",
                                    ],
                                },
                                1: {SS.PROPERTY_NAME_DROPDOWN: None},
                            },
                        },
                        1: {ES.EVENT_NAME_DROPDOWN: None},
                    },
                    CS.COMPLEX_SEGMENT_GROUP_BY: None,
                }
            }
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "segmentation",
        DS.TIME_GROUP_DROPDOWN: 5,
        DS.CUSTOM_DATE_PICKER: [None, None],
        DS.LOOKBACK_WINDOW_DROPDOWN: "1 month",
        MC.TIME_WINDOW_INTERVAL_STEPS: 5,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "user_count",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_dict(res[MS.METRIC_SEGMENTS][0])

    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_OPERATOR_DROPDOWN}, res
    )
    first_property_value_input = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_VALUE_INPUT}, res
    )

    assert first_property_operator_dd is not None
    assert first_property_value_input is not None

    assert first_property_operator_dd["value"] == ">"
    assert first_property_value_input["value"] is None


def test_empty_page_with_project(
    discovered_project: M.DiscoveredProject,
):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {0: {"children": {0: {ES.EVENT_NAME_DROPDOWN: None}}}}
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "segmentation",
        DS.TIME_GROUP_DROPDOWN: 5,
        DS.CUSTOM_DATE_PICKER: [None, None],
        DS.LOOKBACK_WINDOW_DROPDOWN: "1 month",
        MC.TIME_WINDOW_INTERVAL_STEPS: 5,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "user_count",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_segs = to_dict(res[MS.METRIC_SEGMENTS])
    metric_confs = to_dict(res[MC.METRICS_CONFIG_CONTAINER])

    # Metric Segments Part

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": ES.EVENT_NAME_DROPDOWN}, metric_segs
    )
    first_event_dd = find_component_by_id(
        {"index": "0-0", "type": ES.EVENT_NAME_DROPDOWN}, metric_segs
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_NAME_DROPDOWN}, metric_segs
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_OPERATOR_DROPDOWN}, metric_segs
    )
    first_property_value_input = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_VALUE_INPUT}, metric_segs
    )

    assert first_event_dd is not None
    assert first_property_dd is None
    assert second_event_dd is None
    assert first_property_operator_dd is None
    assert first_property_value_input is None

    # Metric Configuration Part

    lookback_dd = find_component_by_id(DS.LOOKBACK_WINDOW_DROPDOWN, metric_confs)
    timegroup_dd = find_component_by_id(DS.TIME_GROUP_DROPDOWN, metric_confs)
    timegroup_dd = find_component_by_id(DS.TIME_GROUP_DROPDOWN, metric_confs)

    assert lookback_dd is not None
    assert lookback_dd["value"] == "1 month"
    assert timegroup_dd is not None
    assert timegroup_dd["value"] == M.TimeGroup.DAY.value


def test_custom_date_selected(discovered_project: M.DiscoveredProject):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {
                0: {
                    "children": {
                        0: {
                            ES.EVENT_NAME_DROPDOWN: "page_visit",
                            "children": {0: {SS.PROPERTY_NAME_DROPDOWN: None}},
                        },
                        1: {ES.EVENT_NAME_DROPDOWN: None},
                    },
                    CS.COMPLEX_SEGMENT_GROUP_BY: None,
                }
            }
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "segmentation",
        DS.TIME_GROUP_DROPDOWN: 5,
        DS.CUSTOM_DATE_PICKER: [None, None],
        DS.LOOKBACK_WINDOW_DROPDOWN: "_custom_date",
        MC.TIME_WINDOW_INTERVAL_STEPS: 5,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "user_count",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_confs = to_dict(res[MC.METRICS_CONFIG_CONTAINER])

    # Metric Segments Part

    date_picker = find_component_by_id(DS.CUSTOM_DATE_PICKER, metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"width": "180px", "display": "inline-block"}


def test_custom_date_selected_new_start_date(discovered_project: M.DiscoveredProject):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {
                0: {
                    "children": {
                        0: {
                            ES.EVENT_NAME_DROPDOWN: "page_visit",
                            "children": {0: {SS.PROPERTY_NAME_DROPDOWN: None}},
                        },
                        1: {ES.EVENT_NAME_DROPDOWN: None},
                    },
                    CS.COMPLEX_SEGMENT_GROUP_BY: None,
                }
            }
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "segmentation",
        DS.TIME_GROUP_DROPDOWN: 5,
        DS.CUSTOM_DATE_PICKER: ["2021-12-01T00:00:00", "2022-01-01T00:00:00"],
        DS.LOOKBACK_WINDOW_DROPDOWN: "_custom_date",
        MC.TIME_WINDOW_INTERVAL_STEPS: 5,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "user_count",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_confs = to_dict(res[MC.METRICS_CONFIG_CONTAINER])

    # Metric Segments Part

    date_picker = find_component_by_id(DS.CUSTOM_DATE_PICKER, metric_confs)

    assert date_picker is not None
    assert date_picker["style"]["display"] == "inline-block"
    assert date_picker["value"] == [
        datetime(2021, 12, 1, 0, 0),
        datetime(2022, 1, 1, 0, 0),
    ]


def test_custom_date_lookback_days_selected(discovered_project: M.DiscoveredProject):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {
                0: {
                    "children": {
                        0: {
                            ES.EVENT_NAME_DROPDOWN: "page_visit",
                            "children": {0: {SS.PROPERTY_NAME_DROPDOWN: None}},
                        },
                        1: {ES.EVENT_NAME_DROPDOWN: None},
                    },
                    CS.COMPLEX_SEGMENT_GROUP_BY: None,
                }
            }
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "segmentation",
        ES.EVENT_NAME_DROPDOWN: ["page_visit", None],
        SS.PROPERTY_OPERATOR_DROPDOWN: [],
        SS.PROPERTY_NAME_DROPDOWN: [None],
        SS.PROPERTY_VALUE_INPUT: [],
        CS.COMPLEX_SEGMENT_GROUP_BY: [None],
        DS.TIME_GROUP_DROPDOWN: 5,
        DS.CUSTOM_DATE_PICKER: ["2021-12-01T00:00:00", "2022-01-01T00:00:00"],
        DS.LOOKBACK_WINDOW_DROPDOWN: "1 month",
        MC.TIME_WINDOW_INTERVAL_STEPS: 5,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "user_count",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    metric_confs = to_dict(res[MC.METRICS_CONFIG_CONTAINER])

    # Metric Segments Part

    date_picker = find_component_by_id(DS.CUSTOM_DATE_PICKER, metric_confs)
    lookback_days_dd = find_component_by_id(DS.LOOKBACK_WINDOW_DROPDOWN, metric_confs)

    assert date_picker is not None
    assert date_picker["style"] == {"width": "180px", "display": "none"}
    assert date_picker["value"] == [None, None]

    assert lookback_days_dd is not None
    assert lookback_days_dd["value"] == "1 month"


def test_mitzu_link_redirected(discovered_project: M.DiscoveredProject, dependencies):
    import flask

    app = flask.Flask("_test_app_")
    with app.app_context():
        flask.current_app.config[DEPS.CONFIG_KEY] = dependencies

        query_params = {
            "m": unquote(
                "eNolTEsKgCAUvIrMulXLLiOm"
                "DxNSI59BiHfPZ5v5MJ%2BGQh6bajgnUhqEy3jSTyiB0fuiYPNf2Z2kq4o58YERsGzhzCvGshhRZqpa6NY21yQvH0z5HoY%3D"
            )
        }
        res = EXP.create_explore_page(query_params, discovered_project)

        explore_page = to_dict(res)

        # Metric Segments Part

        first_event_dd = find_component_by_id(
            {"index": "0-0", "type": ES.EVENT_NAME_DROPDOWN}, explore_page
        )

        second_event_dd = find_component_by_id(
            {"index": "0-1", "type": ES.EVENT_NAME_DROPDOWN}, explore_page
        )
        first_property_dd = find_component_by_id(
            {"index": "0-0-0", "type": SS.PROPERTY_NAME_DROPDOWN}, explore_page
        )
        first_property_operator_dd = find_component_by_id(
            {"index": "0-0-0", "type": SS.PROPERTY_OPERATOR_DROPDOWN}, explore_page
        )

        assert first_event_dd is not None
        assert first_event_dd["value"] == "page_visit"
        assert second_event_dd is not None
        assert first_property_dd is not None
        assert set([option["value"] for option in first_property_dd["options"]]) == set(
            [
                "page_visit.acquisition_campaign",
                "page_visit.domain",
                "page_visit.event_name",
                "page_visit.event_time",
                "page_visit.item_id",
                "page_visit.title",
                "page_visit.user_country_code",
                "page_visit.user_id",
                "page_visit.user_locale",
            ]
        )
        assert first_property_operator_dd is None

        # Metric confs Part

        date_picker = find_component_by_id(DS.CUSTOM_DATE_PICKER, explore_page)
        lookback_days_dd = find_component_by_id(
            DS.LOOKBACK_WINDOW_DROPDOWN, explore_page
        )

        assert date_picker is not None
        assert date_picker["style"]["display"] == "none"
        assert date_picker["value"] == [None, None]
        assert lookback_days_dd is not None
        assert lookback_days_dd["value"] == "2 months"


def test_event_chosen_for_retention(discovered_project: M.DiscoveredProject):
    all_inputs = {
        MS.METRIC_SEGMENTS: {
            "children": {
                0: {"children": {0: {ES.EVENT_NAME_DROPDOWN: "page_visit"}}},
                1: {"children": {0: {ES.EVENT_NAME_DROPDOWN: "checkout"}}},
            }
        },
        H.MITZU_LOCATION: f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore/",
        MTH.METRIC_TYPE_DROPDOWN: "retention",
        DS.TIME_GROUP_DROPDOWN: 1,
        DS.CUSTOM_DATE_PICKER: [None, None],
        DS.LOOKBACK_WINDOW_DROPDOWN: "1 month",
        MC.TIME_WINDOW_INTERVAL_STEPS: 1,
        MC.TIME_WINDOW_INTERVAL: 1,
        MC.AGGREGATION_TYPE: "retention_rate",
        TH.GRAPH_REFRESH_BUTTON: None,
        TH.CHART_TYPE_DD: M.SimpleChartType.LINE.name,
        EXP.METRIC_NAME_INPUT: None,
        EXP.METRIC_ID_VALUE: "test_id",
    }

    res = EXP.handle_input_changes(all_inputs, discovered_project)
    res = to_dict(res[MS.METRIC_SEGMENTS][0])

    second_event_dd = find_component_by_id(
        {"index": "0-1", "type": ES.EVENT_NAME_DROPDOWN}, res
    )
    first_property_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_NAME_DROPDOWN}, res
    )
    first_property_operator_dd = find_component_by_id(
        {"index": "0-0-0", "type": SS.PROPERTY_OPERATOR_DROPDOWN}, res
    )

    assert second_event_dd is not None
    assert first_property_dd is not None
    assert len(first_property_dd["options"]) == 9
    assert first_property_operator_dd is None
