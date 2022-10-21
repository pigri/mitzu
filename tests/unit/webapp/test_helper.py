from mitzu.webapp.helper import transform_all_inputs

ALL_INPUTS = [
    {
        "id": "mitzu_location",
        "property": "href",
        "value": "http: //127.0.0.1:8082/trino_test_project?",
    },
    {"id": "metric-type-dropdown", "property": "value", "value": "conversion"},
    [
        {
            "id": {"index": "0-0", "type": "event_name_dropdown"},
            "property": "value",
            "value": "page_visit",
        },
        {
            "id": {"index": "0-1", "type": "event_name_dropdown"},
            "property": "value",
            "value": None,
        },
        {
            "id": {"index": "1-0", "type": "event_name_dropdown"},
            "property": "value",
            "value": "purchase",
        },
        {
            "id": {"index": "1-1", "type": "event_name_dropdown"},
            "property": "value",
            "value": None,
        },
        {
            "id": {"index": "1-2", "type": "event_name_dropdown"},
            "property": "value",
            "value": None,
        },
        {
            "id": {"index": "2-0", "type": "event_name_dropdown"},
            "property": "value",
            "value": None,
        },
    ],
    [
        {
            "id": {"index": "0-0-0", "type": "property_operator_dropdown"},
            "property": "value",
            "value": "is",
        },
        {
            "id": {"index": "1-0-0", "type": "property_operator_dropdown"},
            "property": "value",
            "value": "is",
        },
        {
            "id": {"index": "1-1-0", "type": "property_operator_dropdown"},
            "property": "value",
            "value": "is",
        },
    ],
    [
        {
            "id": {"index": "0-0-0", "type": "property_name_dropdown"},
            "property": "value",
            "value": "page_visit.user_properties.aquisition_campaign",
        },
        {
            "id": {"index": "0-0-1", "type": "property_name_dropdown"},
            "property": "value",
            "value": None,
        },
        {
            "id": {"index": "1-0-0", "type": "property_name_dropdown"},
            "property": "value",
            "value": "purchase.user_properties.country_code",
        },
        {
            "id": {"index": "1-0-1", "type": "property_name_dropdown"},
            "property": "value",
            "value": None,
        },
        {
            "id": {"index": "1-1-0", "type": "property_name_dropdown"},
            "property": "value",
            "value": "checkout.user_properties.aquisition_campaign",
        },
        {
            "id": {"index": "1-1-1", "type": "property_name_dropdown"},
            "property": "value",
            "value": None,
        },
    ],
    [
        {
            "id": {"index": "0-0-0", "type": "property_value_input"},
            "property": "value",
            "value": ["organic"],
        },
        {
            "id": {"index": "1-0-0", "type": "property_value_input"},
            "property": "value",
            "value": ["cn", "fr"],
        },
        {
            "id": {"index": "1-1-0", "type": "property_value_input"},
            "property": "value",
            "value": ["organic"],
        },
    ],
    [
        {
            "id": {"index": "0", "type": "complex_segment_group_by"},
            "property": "value",
            "value": "page_visit.event_properties.url",
        }
    ],
    {"id": "timegroup_dropdown", "property": "value", "value": 5},
    {"id": "custom_date_picker", "property": "start_date", "value": None},
    {"id": "custom_date_picker", "property": "end_date", "value": None},
    {"id": "lookback_window_dropdown", "property": "value", "value": 30},
    {"id": "conversion_window_interval_steps", "property": "value", "value": 5},
    {"id": "conversion_window_interval", "property": "value", "value": 1},
    {"id": "aggregation_type", "property": "value", "value": "conversion"},
    {"id": "graph_refresh_button", "property": "n_clicks"},
    {"id": "chart_button", "property": "n_clicks"},
    {"id": "table_button", "property": "n_clicks"},
    {"id": "sql_button", "property": "n_clicks"},
]

EXPECTED = {
    "metric_segments": {
        "children": {
            0: {
                "children": {
                    0: {
                        "event_name_dropdown": "page_visit",
                        "children": {
                            0: {
                                "property_operator_dropdown": "is",
                                "property_name_dropdown": "page_visit.user_properties.aquisition_campaign",
                                "property_value_input": ["organic"],
                            },
                            1: {"property_name_dropdown": None},
                        },
                    },
                    1: {"event_name_dropdown": None},
                },
                "complex_segment_group_by": "page_visit.event_properties.url",
            },
            1: {
                "children": {
                    0: {
                        "event_name_dropdown": "purchase",
                        "children": {
                            0: {
                                "property_operator_dropdown": "is",
                                "property_name_dropdown": "purchase.user_properties.country_code",
                                "property_value_input": ["cn", "fr"],
                            },
                            1: {"property_name_dropdown": None},
                        },
                    },
                    1: {
                        "event_name_dropdown": None,
                        "children": {
                            0: {
                                "property_operator_dropdown": "is",
                                "property_name_dropdown": "checkout.user_properties.aquisition_campaign",
                                "property_value_input": ["organic"],
                            },
                            1: {"property_name_dropdown": None},
                        },
                    },
                    2: {"event_name_dropdown": None},
                }
            },
            2: {"children": {0: {"event_name_dropdown": None}}},
        }
    },
    "mitzu_location": "http: //127.0.0.1:8082/trino_test_project?",
    "metric-type-dropdown": "conversion",
    "timegroup_dropdown": 5,
    "custom_date_picker": None,
    "lookback_window_dropdown": 30,
    "conversion_window_interval_steps": 5,
    "conversion_window_interval": 1,
    "aggregation_type": "conversion",
    "graph_refresh_button": None,
    "chart_button": None,
    "table_button": None,
    "sql_button": None,
}


def test_all_inputs_transformation():
    res = transform_all_inputs(ALL_INPUTS)
    assert res == EXPECTED
