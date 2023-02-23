from flask import Flask
import mitzu.webapp.pages.projects.manage_project_component as MPP
import mitzu.webapp.pages.projects.event_tables_config as ETC
import mitzu.webapp.storage as S
from unittest.mock import patch
from typing import Optional, List
from tests.helper import to_dict
import mitzu.webapp.helper as H


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


def create_dummy_tbl_body(checkbox1: bool, checkbox2: bool) -> List:
    return [
        get_table_row(
            full_table_name="database.test_table_1",
            check_box=checkbox1,
            user_id_column="test_user_id_col_1",
            event_time_column="test_event_time_col_1",
            event_name_column="test_event_name_col_1",
            date_partition_column="test_date_partition_col_1",
            ignore_cols="col1,col2",
        ),
        get_table_row(
            full_table_name="database.test_table_2",
            check_box=checkbox2,
            user_id_column="test_user_id_col_1",
            event_time_column="test_event_time_col_1",
            event_name_column="test_event_name_col_1",
            date_partition_column="test_date_partition_col_1",
            ignore_cols="col1,col2",
        ),
    ]


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_evt_table_checkboxes_select_all(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.TBL_HEADER_CHECK_BOX_TYPE

        res = ETC.manage_table_checkboxes(True, [], create_dummy_tbl_body(True, False))

        assert res[0] is False
        assert res[1] is False
        assert res[2] == [True, True]


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_evt_table_checkboxes_deselect_all(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.TBL_HEADER_CHECK_BOX_TYPE

        res = ETC.manage_table_checkboxes(False, [], create_dummy_tbl_body(True, True))

        assert res[0] is True
        assert res[1] is True
        assert res[2] == [False, False]


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_evt_table_checkboxes_deselect_tr_checkboxes(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = {
            "type": ETC.TBL_CHECK_BOX_TYPE,
            "index": "database.test_table_1",
        }

        res = ETC.manage_table_checkboxes(True, [], create_dummy_tbl_body(False, False))

        assert res[0] is True
        assert res[1] is True
        assert res[2] == [False, False]


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_choose_schema_dropdown(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.ADD_TABLES_BUTTON

        res = ETC.manage_choose_schema_dropdown(1, 0, 0, S.SAMPLE_CONNECTION_ID)

        assert res[0] == [{"label": "main", "value": "main"}]
        assert res[2] == "Select schema"


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_add_tables_schema_chosen(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.CHOOSE_SCHEMA_DD

        res = ETC.manage_choose_tables_checklist(
            schema="main",
            connection_id=S.SAMPLE_CONNECTION_ID,
            select_all_clicks=0,
            close=0,
            confirm=0,
            options=[],
            selected_values=[],
        )

        assert res is not None
        assert res[0] == [
            {"label": "add_to_carts", "value": "add_to_carts"},
            {"label": "checkouts", "value": "checkouts"},
            {"label": "email_opened_events", "value": "email_opened_events"},
            {"label": "email_sent_events", "value": "email_sent_events"},
            {"label": "email_subscriptions", "value": "email_subscriptions"},
            {"label": "page_events", "value": "page_events"},
            {"label": "search_events", "value": "search_events"},
        ]
        assert res[2] == "Choose tables to add"


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_configure_properties(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.CONFIGURE_TABLES_BUTTON
        tbl_body = [
            get_table_row(
                full_table_name="main.page_events",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="acquisition_campaign,user_locale",
            ),
            get_table_row(
                full_table_name="main.add_to_carts",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="",
                date_partition_column="",
                ignore_cols="user_locale,item",
            ),
            get_table_row(
                full_table_name="main.search_events",
                check_box=False,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="",
            ),
        ]
        res = ETC.manage_configure_property_inputs(
            close=0,
            configure=0,
            confirm=0,
            set_progress=lambda *args: print(args),
            connection_id=S.SAMPLE_CONNECTION_ID,
            tbl_body_children=tbl_body,
        )

        assert res[0][0] == [
            {"label": "event_name ", "value": "event_name"},
            {"label": "event_time ", "value": "event_time"},
            {"label": "user_country_code ", "value": "user_country_code"},
            {"label": "user_id ", "value": "user_id"},
            {"label": "user_locale ", "value": "user_locale"},
            {
                "label": "acquisition_campaign (missing from 1)",
                "value": "acquisition_campaign",
            },
            {"label": "domain (missing from 1)", "value": "domain"},
            {"label": "item (missing from 1)", "value": "item"},
            {"label": "item_id (missing from 1)", "value": "item_id"},
            {"label": "title (missing from 1)", "value": "title"},
        ]

        assert res[1][0] == ["user_id"]
        assert res[1][1] == ["event_time"]
        assert res[1][2] == ["event_name"]

        ignore_cols = list(res[1][4])
        ignore_cols.sort()
        assert ignore_cols == ["acquisition_campaign", "item", "user_locale"]

        assert res[2] == [
            "Select column",
            "Select column",
            "Optional",
            "Optional",
            "Optional",
        ]
        assert res[3] == "Select properties for 2 tables:"


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_configure_properties_table_was_deleted(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.CONFIGURE_TABLES_BUTTON
        tbl_body = [
            get_table_row(
                full_table_name="main.DELETED_TABLE",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="acquisition_campaign,user_locale",
            ),
        ]
        res = ETC.manage_configure_property_inputs(
            close=0,
            configure=0,
            confirm=0,
            set_progress=lambda *args: print(args),
            connection_id=S.SAMPLE_CONNECTION_ID,
            tbl_body_children=tbl_body,
        )

        assert res[0][0] == []
        assert res[1] == [None, None, None, None, None]
        assert res[2] == [None, None, None, None, None]
        assert (
            res[3]
            == "Something went wrong. Make sure your selected tables are validated."
        )


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_event_data_table_body_tables_added(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.ADD_TABLES_MODAL_CONFIRM
        tbl_body = [
            get_table_row(
                full_table_name="main.page_events",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="acquisition_campaign,user_locale",
            ),
        ]
        res = ETC.manage_event_data_table_body(
            set_progress=lambda *args: print(args),
            add_tables=0,
            configure_table=0,
            remove_tables=0,
            search_value=0,
            validate_nclicks=0,
            choose_schema_dd_value="main",
            tables=["page_events", "add_to_carts"],
            tbl_body_children=tbl_body,
            edt_properties=[],
            connection_id=S.SAMPLE_CONNECTION_ID,
        )
        second_table = to_dict(res[1])

        assert len(res) == 2
        assert res[0]["props"]["children"][1]["props"]["children"] == "main.page_events"
        assert res[0]["props"]["children"][2]["props"]["children"] == "user_id"
        assert res[0]["props"]["children"][3]["props"]["children"] == "event_time"

        assert second_table["props"]["children"][1].children == "main.add_to_carts"
        assert second_table["props"]["children"][2].children == "<Required>"
        assert second_table["props"]["children"][3].children == "<Required>"


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_event_data_validated(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.EDT_VALIDATE_BUTTON
        tbl_body = [
            get_table_row(
                full_table_name="main.page_events",
                check_box=True,
                user_id_column="user_id_xxx",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="acquisition_campaign,user_locale",
            ),
            get_table_row(
                full_table_name="main.DELETED_TABLE",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="acquisition_campaign,user_locale",
            ),
        ]
        res = ETC.manage_event_data_table_body(
            set_progress=lambda *args: print(args),
            add_tables=0,
            configure_table=0,
            remove_tables=0,
            search_value=0,
            validate_nclicks=1,
            choose_schema_dd_value="",
            tables=[],
            tbl_body_children=tbl_body,
            edt_properties=[],
            connection_id=S.SAMPLE_CONNECTION_ID,
        )

        assert len(res) == 2
        assert res[0]["props"]["children"][1]["props"]["children"] == "main.page_events"
        assert res[0]["props"]["children"][2]["props"]["children"] == "user_id_xxx"
        assert res[0]["props"]["children"][2]["props"]["className"] == H.TBL_CLS_WARNING
        assert res[0]["props"]["children"][3]["props"]["className"] == H.TBL_CLS

        assert (
            res[1]["props"]["children"][1]["props"]["children"] == "main.DELETED_TABLE"
        )
        assert res[1]["props"]["children"][1]["props"]["className"] == H.TBL_CLS_WARNING
        assert res[1]["props"]["children"][2]["props"]["children"] == "user_id"
        assert res[1]["props"]["children"][2]["props"]["className"] == H.TBL_CLS_WARNING


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_event_data_table_body_remove_tables(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.REMOVE_TABLES_BUTTON
        tbl_body = [
            get_table_row(
                full_table_name="main.page_events",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="",
            ),
            get_table_row(
                full_table_name="main.add_to_carts",
                check_box=False,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="",
            ),
        ]
        res = ETC.manage_event_data_table_body(
            set_progress=lambda *args: print(args),
            add_tables=0,
            configure_table=0,
            remove_tables=1,
            search_value=0,
            validate_nclicks=0,
            choose_schema_dd_value="",
            tables=[],
            tbl_body_children=tbl_body,
            edt_properties=[],
            connection_id=S.SAMPLE_CONNECTION_ID,
        )

        assert len(res) == 1

        assert (
            res[0]["props"]["children"][1]["props"]["children"] == "main.add_to_carts"
        )


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_event_data_table_body_search_tables(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.TBL_SEARCH_INPUT
        tbl_body = [
            get_table_row(
                full_table_name="main.page_events",
                check_box=True,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="",
            ),
            get_table_row(
                full_table_name="main.add_to_carts",
                check_box=False,
                user_id_column="user_id",
                event_time_column="event_time",
                event_name_column="event_name",
                date_partition_column="",
                ignore_cols="",
            ),
        ]
        res = ETC.manage_event_data_table_body(
            set_progress=lambda *args: print(args),
            add_tables=0,
            configure_table=0,
            remove_tables=1,
            search_value="add",
            validate_nclicks=0,
            choose_schema_dd_value="",
            tables=[],
            tbl_body_children=tbl_body,
            edt_properties=[],
            connection_id=S.SAMPLE_CONNECTION_ID,
        )

        assert len(res) == 2

        assert res[0]["props"]["style"] == {"display": "none"}
        assert res[1]["props"]["style"] == {"display": "table-row"}


@patch("mitzu.webapp.pages.projects.event_tables_config.ctx")
def test_manage_event_data_table_body_configure_properties(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = ETC.CONF_TABLES_MODAL_CONFIRM
        tbl_body = [
            get_table_row(
                full_table_name="main.page_events",
                check_box=True,
                user_id_column="",
                event_time_column="",
                event_name_column="",
                date_partition_column="",
                ignore_cols="",
            ),
            get_table_row(
                full_table_name="main.add_to_carts",
                check_box=True,
                user_id_column="",
                event_time_column="",
                event_name_column="",
                date_partition_column="",
                ignore_cols="",
            ),
        ]
        res_body = ETC.manage_event_data_table_body(
            set_progress=lambda *args: print(args),
            add_tables=0,
            configure_table=0,
            remove_tables=0,
            search_value="add",
            validate_nclicks=0,
            choose_schema_dd_value="",
            tables=[],
            tbl_body_children=tbl_body,
            edt_properties=[
                ["user_id"],
                ["event_time"],
                ["event_name"],
                [],
                ["item", "acquisition_campaign"],
            ],
            connection_id=S.SAMPLE_CONNECTION_ID,
        )

        res = to_dict(res_body)
        assert len(res) == 2

        assert res[0]["children"][2]["props"]["children"] == "user_id"
        assert res[0]["children"][3]["props"]["children"] == "event_time"
        assert res[0]["children"][4]["props"]["children"] == "event_name"
        assert res[0]["children"][5]["props"]["children"] is None
        assert res[0]["children"][6]["props"]["children"] == "acquisition_campaign"

        assert res[1]["children"][2]["props"]["children"] == "user_id"
        assert res[1]["children"][3]["props"]["children"] == "event_time"
        assert res[1]["children"][4]["props"]["children"] == "event_name"
        assert res[1]["children"][5]["props"]["children"] is None
        assert res[1]["children"][6]["props"]["children"] == "item"
