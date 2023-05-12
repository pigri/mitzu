from flask import Flask
from tests.helper import to_dict, find_component_by_id
import mitzu.webapp.pages.projects.manage_project_component as MPP

import mitzu.webapp.pages.projects.event_tables_config as ETC
import mitzu.webapp.pages.manage_project as PRJ
import mitzu.webapp.storage as S
from datetime import datetime
import mitzu.model as M
from unittest.mock import patch
from mitzu.webapp.dependencies import Dependencies
from typing import Optional
from dash import no_update


def get_project_input_group_args(**kwargs):
    values = {
        MPP.PROP_PROJECT_ID: S.SAMPLE_PROJECT_ID,
        MPP.PROP_PROJECT_NAME: "Sample ecommerce project",
        MPP.PROP_CONNECTION: S.SAMPLE_CONNECTION_ID,
        MPP.PROP_DESCRIPTION: "Sample description",
        MPP.PROP_DISC_SAMPLE_SIZE: 1000,
        MPP.PROP_DISC_LOOKBACK_DAYS: 365,
        MPP.PROP_EXPLORE_AUTO_REFRESH: True,
        MPP.PROP_END_DATE_CONFIG: "CUSTOM_DATE",
        MPP.PROP_CUSTOM_END_DATE_CONFIG: "2022-01-01 00:00:00",
    }
    for k, v in kwargs.items():
        values[k] = v

    return [
        {
            "id": {"index": key, "type": MPP.PROJECT_INDEX_TYPE},
            "property": "value",
            "value": value,
        }
        for key, value in values.items()
    ]


def get_table_row(
    full_table_name: str,
    check_box: bool,
    user_id_column: str,
    event_time_column: str,
    date_partition_column: Optional[str],
    event_name_column: Optional[str],
    ignore_cols: Optional[str],
):
    return {
        "props": {
            "children": [
                {
                    "props": {"children": {"props": {"value": check_box}}},
                },
                {"props": {"children": full_table_name}},
                {"props": {"children": user_id_column}},
                {"props": {"children": event_time_column}},
                {"props": {"children": event_name_column}},
                {"props": {"children": date_partition_column}},
                {"props": {"children": ignore_cols}},
            ]
        }
    }


def test_create_new_project(server: Flask):
    with server.test_request_context():
        connections_comp = PRJ.layout(None)
        comp_dict = to_dict(connections_comp)

        #  There is no business logic to test in this layout.
        assert comp_dict is not None

        name_input = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_PROJECT_NAME},
            comp_dict,
        )
        assert name_input is not None
        assert name_input["value"] is None

        desc_input = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_DESCRIPTION},
            comp_dict,
        )
        assert desc_input is not None
        assert desc_input["value"] is None
        assert desc_input["placeholder"] == "Describe the project!"


def test_load_exiting_project(server: Flask):
    with server.test_request_context():
        comp_dict = to_dict(PRJ.layout(project_id=S.SAMPLE_PROJECT_ID))

        #  There is no business logic to test in this layout.
        assert comp_dict is not None

        # Project name
        name_input = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_PROJECT_NAME},
            comp_dict,
        )
        assert name_input is not None
        assert name_input["value"] == "Sample ecommerce project"

        # Connection
        conn_input = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_CONNECTION},
            comp_dict,
        )
        assert conn_input is not None
        assert conn_input["value"] == S.SAMPLE_CONNECTION_ID

        # Description
        desc_input = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_DESCRIPTION},
            comp_dict,
        )
        assert desc_input is not None
        assert desc_input["value"] is None
        assert desc_input["placeholder"] == "Describe the project!"

        # Event tables
        tbl_body = to_dict(find_component_by_id(ETC.EDT_TBL_BODY, comp_dict))

        assert tbl_body is not None
        assert len(tbl_body["children"]) == 7
        # Table properties
        assert (
            tbl_body["children"][1]["children"][1]["children"] == "main.search_events"
        )
        assert tbl_body["children"][1]["children"][2]["children"] == "user_id"
        assert tbl_body["children"][1]["children"][3]["children"] == "event_time"
        assert tbl_body["children"][2]["children"][1]["children"] == "main.add_to_carts"

        # Webapp Settings
        auto_refresh = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_EXPLORE_AUTO_REFRESH},
            comp_dict,
        )
        assert auto_refresh is not None
        assert auto_refresh["value"] is True
        end_dt_config = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_END_DATE_CONFIG},
            comp_dict,
        )
        assert end_dt_config is not None
        assert end_dt_config["value"] == "CUSTOM_DATE"
        custom_end_dt = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_CUSTOM_END_DATE_CONFIG},
            comp_dict,
        )
        assert custom_end_dt is not None
        assert custom_end_dt["value"] == datetime(2022, 1, 1, 0, 0)

        # Discovery Settings
        d_lookback = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_DISC_LOOKBACK_DAYS},
            comp_dict,
        )
        assert d_lookback is not None
        assert d_lookback["value"] == 365

        d_sample_size = find_component_by_id(
            {"type": MPP.PROJECT_INDEX_TYPE, "index": MPP.PROP_DISC_SAMPLE_SIZE},
            comp_dict,
        )
        assert d_sample_size is not None
        assert d_sample_size["value"] == 1000


@patch("mitzu.webapp.pages.manage_project.ctx")
def test_save_button_clicked_deleted_rows(
    ctx, server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        ctx.args_grouping = [
            [],
            [],
            [],
            get_project_input_group_args(**{MPP.PROP_PROJECT_NAME: "TEST_RENAME"}),
        ]
        ctx.triggered_id = PRJ.SAVE_BUTTON
        (project_info, redirection) = PRJ.save_button_clicked(
            save_clicks=1,
            save_and_discover_clicks=0,
            edt_table_rows=[],  # Rows
            prop_values=[],
            pathname="http://localhost:8082/projects/sample_project_id/manage/",
        )

        assert redirection == no_update
        assert project_info == "Project succesfully saved"
        saved_project = dependencies.storage.get_project(S.SAMPLE_PROJECT_ID)
        assert len(saved_project.event_data_tables) == 0
        assert saved_project.project_name == "TEST_RENAME"


@patch("mitzu.webapp.pages.manage_project.ctx")
def test_save_button_clicked_add_2_rows(ctx, server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        ctx.args_grouping = [
            [],
            [],
            [],
            get_project_input_group_args(),
        ]
        ctx.triggered_id = PRJ.SAVE_BUTTON
        new_rows = [
            get_table_row(
                full_table_name="main.page_events",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="acquisition_campaign,title",
            ),
            get_table_row(
                full_table_name="main.checkouts",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="",
                date_partition_column="",
                ignore_cols="",
            ),
        ]
        (project_info, redirection) = PRJ.save_button_clicked(
            save_clicks=1,
            save_and_discover_clicks=0,
            edt_table_rows=new_rows,  # Rows
            prop_values=[],
            pathname="http://localhost:8082/projects/sample_project_id/manage/",
        )

        assert redirection == no_update
        assert project_info == "Project succesfully saved"
        saved_project = dependencies.storage.get_project(S.SAMPLE_PROJECT_ID)
        assert len(saved_project.event_data_tables) == 2

        edt_1 = saved_project.event_data_tables[0]
        assert edt_1.table_name == "page_events"
        assert edt_1.schema == "main"
        assert edt_1.user_id_field == M.Field(
            _name="user_id", _sub_fields=None, _type=M.DataType.STRING
        )

        assert edt_1.event_time_field == M.Field(
            _name="event_time", _sub_fields=None, _type=M.DataType.DATETIME
        )
        assert edt_1.event_name_field == M.Field(
            _name="event_name", _sub_fields=None, _type=M.DataType.STRING
        )
        assert edt_1.date_partition_field is None
        assert edt_1.ignored_fields == [
            M.Field(
                _name="acquisition_campaign", _sub_fields=None, _type=M.DataType.STRING
            ),
            M.Field(_name="title", _sub_fields=None, _type=M.DataType.STRING),
        ]

        edt_2 = saved_project.event_data_tables[1]
        assert edt_2.table_name == "checkouts"
        assert edt_2.schema == "main"
        assert edt_2.user_id_field == M.Field(
            _name="user_id", _sub_fields=None, _type=M.DataType.STRING
        )
        assert edt_2.date_partition_field is None
        assert edt_2.event_name_field is None
        assert edt_2.event_name_alias == "checkouts"
        assert edt_2.event_time_field == M.Field(
            _name="event_time", _sub_fields=None, _type=M.DataType.DATETIME
        )
        assert edt_2.ignored_fields == []


@patch("mitzu.webapp.pages.manage_project.ctx")
def test_save_and_discover_button_clicked_deleted_rows(
    ctx, server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        ctx.args_grouping = [
            [],
            [],
            [],
            get_project_input_group_args(**{MPP.PROP_PROJECT_NAME: "TEST_RENAME"}),
        ]
        ctx.triggered_id = PRJ.SAVE_AND_DISCOVER_BUTTON
        (project_info, redirection) = PRJ.save_button_clicked(
            save_clicks=0,
            save_and_discover_clicks=1,
            edt_table_rows=[],  # Rows
            prop_values=[],
            pathname="http://localhost:8082/projects/sample_project_id/manage/",
        )

        assert redirection == f"/events/{S.SAMPLE_PROJECT_ID}"
        assert project_info == no_update
