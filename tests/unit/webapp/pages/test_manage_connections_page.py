from flask import Flask
from tests.helper import to_dict, find_component_by_id
from mitzu.webapp.dependencies import Dependencies
from mitzu.webapp.storage import SAMPLE_PROJECT_ID
import mitzu.webapp.pages.connections.manage_connections_component as MCC
import mitzu.webapp.pages.manage_connection as MC
from unittest.mock import patch
import mitzu.webapp.pages.paths as P
import mitzu.model as M
from dash import no_update
from typing import cast
from unittest.mock import MagicMock


def get_connections_input_arg_groupping(**kwargs):
    values = {
        "connection_id": "sample_project_id",
        "connection_name": "Sample project",
        "connection_type": "SQLITE",
        "host": "sample_project",
        "port": None,
        "catalog": None,
        "username": None,
        "password": None,
    }
    for k, v in kwargs.items():
        values[k] = v

    return [
        {
            "id": {"index": key, "type": MC.CONPAGE_INDEX_TYPE},
            "property": "value",
            "value": value,
        }
        for key, value in values.items()
    ]


def test_create_new_connection(server: Flask):
    with server.test_request_context():
        connections_comp = MC.layout(None)
        comp_dict = to_dict(connections_comp)

        #  There is no business logic to test in this layout.
        assert comp_dict is not None

        name_input = find_component_by_id(
            {"type": MC.CONPAGE_INDEX_TYPE, "index": MCC.PROP_CONNECTION_NAME},
            comp_dict,
        )
        assert name_input is not None
        assert name_input["value"] == "New connection"


def test_connection_update_layout_values_are_filled(
    server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        con_id = dependencies.storage.list_connections()[0]

        connections_comp = MC.layout(con_id)
        comp_dict = to_dict(connections_comp)

        #  There is no business logic to test in this layout.
        assert comp_dict is not None

        name_input = find_component_by_id(
            {"type": MC.CONPAGE_INDEX_TYPE, "index": MCC.PROP_CONNECTION_NAME},
            comp_dict,
        )
        assert name_input is not None
        assert name_input["value"] == "Sample connection"

        con_type_input = find_component_by_id(
            {"type": MC.CONPAGE_INDEX_TYPE, "index": MCC.PROP_CONNECTION_TYPE},
            comp_dict,
        )
        assert con_type_input is not None
        assert con_type_input["value"] == "SQLITE"
        assert con_type_input["data"] == [
            {"label": "Athena", "value": "ATHENA"},
            {"label": "Trino", "value": "TRINO"},
            {"label": "Postgresql", "value": "POSTGRESQL"},
            {"label": "Redshift", "value": "REDSHIFT"},
            {"label": "Mysql", "value": "MYSQL"},
            {"label": "Sqlite", "value": "SQLITE"},
            {"label": "Databricks", "value": "DATABRICKS"},
            {"label": "Snowflake", "value": "SNOWFLAKE"},
            {"label": "Bigquery", "value": "BIGQUERY"},
        ]


def test_connection_button_missing_info_failes(
    server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        agrs_groupping = get_connections_input_arg_groupping(connection_name=None)

        # Values are passed through dash.ctx.args_grouping
        res = to_dict(
            MCC.test_connection_clicked(1, MC.CONPAGE_CC_CONFIG, agrs_groupping)
        )
        assert res == {
            "props": {
                "children": "Failed to connect: Connection name can't be empty",
                "className": "my-3 text-danger",
            },
            "type": "P",
            "namespace": "dash_html_components",
            "children": "Failed to connect: Connection name can't be empty",
        }


def test_connection_successfull_to_sqlite(server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        agrs_groupping = get_connections_input_arg_groupping()
        # Values are passed through dash.ctx.args_grouping
        res = to_dict(
            MCC.test_connection_clicked(1, MC.CONPAGE_CC_CONFIG, agrs_groupping)
        )
        assert res == {
            "props": {"children": "Connected successfully!", "className": "lead my-3"},
            "type": "P",
            "namespace": "dash_html_components",
            "children": "Connected successfully!",
        }


def test_delete_confirmed(server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        con_id = dependencies.storage.list_connections()[0]
        for project_info in dependencies.storage.list_projects():
            project = dependencies.storage.get_project(project_info.id)
            if project.get_connection_id() == con_id:
                dependencies.storage.delete_project(project.id)
        res = to_dict(
            MC.delete_confirmed_clicked(
                1,
                P.create_path(P.CONNECTIONS_MANAGE_PATH, connection_id=con_id),
            )
        )
        # Dummy response from callback
        assert res == 1

        assert len(dependencies.storage.list_connections()) == 0


@patch("mitzu.webapp.pages.manage_connection.ctx")
def test_delete_button_clicked_connection_is_used(
    ctx, server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        ctx.triggered_id = MC.CONNECTION_DELETE_BUTTON
        con_id = dependencies.storage.list_connections()[0]
        res, info = MC.delete_button_clicked(
            1,
            0,
            P.create_path(P.CONNECTIONS_MANAGE_PATH, connection_id=con_id),
        )
        # Dummy response from callback
        assert res is False
        info_component = to_dict(info)["children"]
        assert info_component is not None
        assert (
            info_component[0]["children"]
            == "You can't delete this connection because it is used by  "
        )
        assert info_component[1]["children"] == "Sample ecommerce project"
        assert (
            info_component[1]["props"]["href"] == "/projects/sample_project_id/manage"
        )


@patch("mitzu.webapp.pages.manage_connection.ctx")
def test_delete_button_clicked_connection_is_not_used(
    ctx, server: Flask, dependencies: Dependencies
):
    dependencies.storage.delete_project(SAMPLE_PROJECT_ID)
    with server.test_request_context():
        ctx.triggered_id = MC.CONNECTION_DELETE_BUTTON
        con_id = dependencies.storage.list_connections()[0]
        res, info = MC.delete_button_clicked(
            1,
            0,
            P.create_path(P.CONNECTIONS_MANAGE_PATH, connection_id=con_id),
        )
        # Dummy response from callback
        assert res is True
        assert info == ""


@patch("mitzu.webapp.pages.manage_connection.ctx")
def test_delete_button_clicked_for_non_existing_connection(
    ctx, server: Flask, dependencies: Dependencies
):
    cons = dependencies.storage.list_connections()
    with server.test_request_context():
        ctx.triggered_id = MC.CONNECTION_DELETE_BUTTON
        res, info = MC.delete_button_clicked(
            1,
            0,
            P.create_path(P.CONNECTIONS_MANAGE_PATH, connection_id="none-existing"),
        )
        # Dummy response from callback
        assert res is True
        assert info == ""
        assert cons == dependencies.storage.list_connections()


@patch("mitzu.webapp.pages.manage_connection.ctx")
def test_save_connection_button_clicked(ctx, server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        ctx.args_grouping = [
            [],
            get_connections_input_arg_groupping(
                connection_id="new_connection_id", connection_name="New Connection"
            ),
        ]
        ctx.triggered_id = MC.CONNECTION_SAVE_BUTTON
        # Values are passed through dash.ctx.args_grouping
        res = to_dict(MC.save_button_clicked(1, {}))
        assert res == [
            {
                "props": {"children": "Connection saved", "className": "lead"},
                "type": "P",
                "namespace": "dash_html_components",
                "children": "Connection saved",
            },
            no_update,
        ]

        assert len(dependencies.storage.list_connections()) == 2
        con = dependencies.storage.get_connection("new_connection_id")
        assert con.connection_name == "New Connection"
        assert con.id == "new_connection_id"
        assert con.connection_type == M.ConnectionType.SQLITE
        cast(
            MagicMock, dependencies.tracking_service
        ).track_connection_saved.assert_called_with(con)


@patch("mitzu.webapp.pages.manage_connection.ctx")
def test_save_connection_button_clicked_invalid(
    ctx, server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        ctx.args_grouping = [
            [],
            get_connections_input_arg_groupping(connection_name=None),
        ]
        # Values are passed through dash.ctx.args_grouping
        res = to_dict(MC.save_button_clicked(1, {}))[0]

        assert res == {
            "props": {
                "children": "Saving failed: Connection name can't be empty",
                "className": "lead text-danger",
            },
            "type": "P",
            "namespace": "dash_html_components",
            "children": "Saving failed: Connection name can't be empty",
        }

        assert len(dependencies.storage.list_connections()) == 1


def test_connection_type_changed(server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        # ATHENA
        res = to_dict(
            MCC.connection_type_changed(
                M.ConnectionType.ATHENA.name,
                P.CONNECTIONS_CREATE_PATH,
                MC.CONPAGE_CC_CONFIG,
            )
        )
        assert (
            find_component_by_id(
                {"type": MC.CONPAGE_INDEX_TYPE, "index": MCC.PROP_REGION}, res
            )
            is not None
        )
        assert (
            find_component_by_id(
                {"type": MC.CONPAGE_INDEX_TYPE, "index": MCC.PROP_S3_STAGING_DIR}, res
            )
            is not None
        )

        #  SNOWFLAKE
        res = to_dict(
            MCC.connection_type_changed(
                M.ConnectionType.SNOWFLAKE.name,
                P.CONNECTIONS_CREATE_PATH,
                MC.CONPAGE_CC_CONFIG,
            )
        )
        assert (
            find_component_by_id(
                {"type": MC.CONPAGE_INDEX_TYPE, "index": MCC.PROP_WAREHOUSE},
                res,
            )
            is not None
        )

        # DATABRICKS
        res = to_dict(
            MCC.connection_type_changed(
                M.ConnectionType.DATABRICKS.name,
                P.CONNECTIONS_CREATE_PATH,
                MC.CONPAGE_CC_CONFIG,
            )
        )
        assert (
            find_component_by_id(
                {"type": MC.CONPAGE_INDEX_TYPE, "index": MCC.PROP_HTTP_PATH}, res
            )
            is not None
        )
