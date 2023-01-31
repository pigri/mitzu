from flask import Flask
from tests.helper import to_dict, find_component_by_id
from mitzu.webapp.dependencies import Dependencies
import mitzu.webapp.pages.connections.manage_connections_component as MCC
import mitzu.webapp.pages.manage_connection as CON
from unittest.mock import patch
import mitzu.webapp.pages.paths as P
import mitzu.model as M


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
            "id": {"index": key, "type": "connection_property"},
            "property": "value",
            "value": value,
        }
        for key, value in values.items()
    ]


def test_create_new_connection(server: Flask):
    with server.test_request_context():
        connections_comp = CON.layout(None)
        comp_dict = to_dict(connections_comp)

        #  There is no business logic to test in this layout.
        assert comp_dict is not None

        name_input = find_component_by_id(
            {"type": MCC.INDEX_TYPE, "index": MCC.PROP_CONNECTION_NAME}, comp_dict
        )
        assert name_input is not None
        assert name_input["value"] is None


def test_connection_update_layout_values_are_filled(
    server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        con_id = dependencies.storage.list_connections()[0]

        connections_comp = CON.layout(con_id)
        comp_dict = to_dict(connections_comp)

        #  There is no business logic to test in this layout.
        assert comp_dict is not None

        name_input = find_component_by_id(
            {"type": MCC.INDEX_TYPE, "index": MCC.PROP_CONNECTION_NAME}, comp_dict
        )
        assert name_input is not None
        assert name_input["value"] == "Sample connection"

        con_type_input = find_component_by_id(
            {"type": MCC.INDEX_TYPE, "index": MCC.PROP_CONNECTION_TYPE}, comp_dict
        )
        assert con_type_input is not None
        assert con_type_input["value"] == "SQLITE"
        assert con_type_input["data"] == [
            {"label": "Athena", "value": "ATHENA"},
            {"label": "Trino", "value": "TRINO"},
            {"label": "Postgresql", "value": "POSTGRESQL"},
            {"label": "Mysql", "value": "MYSQL"},
            {"label": "Databricks", "value": "DATABRICKS"},
            {"label": "Snowflake", "value": "SNOWFLAKE"},
            {"label": "Sqlite", "value": "SQLITE"},
        ]


@patch("mitzu.webapp.pages.connections.manage_connections_component.ctx")
def test_connection_button_missing_info_failes(
    ctx, server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        ctx.args_grouping = [
            [],
            get_connections_input_arg_groupping(connection_name=None),
        ]

        # Values are passed through dash.ctx.args_grouping
        res = to_dict(MCC.test_connection_clicked(1, {}, ""))
        assert res == {
            "props": {"children": "Invalid Connection Name", "className": "lead my-3"},
            "type": "P",
            "namespace": "dash_html_components",
            "children": "Invalid Connection Name",
        }


@patch("mitzu.webapp.pages.connections.manage_connections_component.ctx")
def test_connection_successfull_to_sqlite(
    ctx, server: Flask, dependencies: Dependencies
):
    with server.test_request_context():
        ctx.args_grouping = [[], get_connections_input_arg_groupping()]
        # Values are passed through dash.ctx.args_grouping
        res = to_dict(MCC.test_connection_clicked(1, {}, ""))
        assert res == {
            "props": {"children": "Connected successfully!", "className": "lead my-3"},
            "type": "P",
            "namespace": "dash_html_components",
            "children": "Connected successfully!",
        }


@patch("mitzu.webapp.pages.connections.manage_connections_component.ctx")
def test_delete_confirmed(ctx, server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        con_id = dependencies.storage.list_connections()[0]
        res = to_dict(
            MCC.delete_confirmed_clicked(
                1,
                P.create_path(P.CONNECTIONS_MANAGE_PATH, connection_id=con_id),
            )
        )
        # Dummy response from callback
        assert res == 1

        assert len(dependencies.storage.list_connections()) == 0


@patch("mitzu.webapp.pages.manage_connection.ctx")
def test_save_connection_button_clicked(ctx, server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        ctx.args_grouping = [
            [],
            get_connections_input_arg_groupping(
                connection_id="new_connection_id", connection_name="New Connection"
            ),
        ]
        # Values are passed through dash.ctx.args_grouping
        res = to_dict(CON.save_button_clicked(1, {}, ""))
        assert res == [
            {
                "props": {"children": "Connection saved", "className": "lead"},
                "type": "P",
                "namespace": "dash_html_components",
                "children": "Connection saved",
            }
        ]

        assert len(dependencies.storage.list_connections()) == 2
        con = dependencies.storage.get_connection("new_connection_id")
        assert con.connection_name == "New Connection"
        assert con.id == "new_connection_id"
        assert con.connection_type == M.ConnectionType.SQLITE


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
        res = to_dict(CON.save_button_clicked(1, {}, ""))
        assert res == {
            "props": {"children": "Invalid Connection Name", "className": "lead"},
            "type": "P",
            "namespace": "dash_html_components",
            "children": "Invalid Connection Name",
        }

        assert len(dependencies.storage.list_connections()) == 1


def test_connection_type_changed(server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        # ATHENA
        res = to_dict(
            MCC.connection_type_changed(
                M.ConnectionType.ATHENA.name, P.CONNECTIONS_CREATE_PATH
            )
        )
        assert (
            find_component_by_id(
                {"type": MCC.INDEX_TYPE, "index": MCC.PROP_REGION}, res
            )
            is not None
        )
        assert (
            find_component_by_id(
                {"type": MCC.INDEX_TYPE, "index": MCC.PROP_S3_STAGING_DIR}, res
            )
            is not None
        )

        #  SNOWFLAKE
        res = to_dict(
            MCC.connection_type_changed(
                M.ConnectionType.SNOWFLAKE.name, P.CONNECTIONS_CREATE_PATH
            )
        )
        assert (
            find_component_by_id(
                {"type": MCC.INDEX_TYPE, "index": MCC.PROP_WAREHOUSE}, res
            )
            is not None
        )

        # DATABRICKS
        res = to_dict(
            MCC.connection_type_changed(
                M.ConnectionType.DATABRICKS.name, P.CONNECTIONS_CREATE_PATH
            )
        )
        assert (
            find_component_by_id(
                {"type": MCC.INDEX_TYPE, "index": MCC.PROP_HTTP_PATH}, res
            )
            is not None
        )
