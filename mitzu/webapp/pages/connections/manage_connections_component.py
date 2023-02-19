from typing import Any, Dict, List, Optional, cast, Tuple
import flask
from uuid import uuid4

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from dash import Input, Output, State, callback, ctx, html, no_update, ALL, dcc

import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.paths as P
import mitzu.webapp.configs as CO
from mitzu.helper import value_to_label
from mitzu.webapp.auth.decorator import restricted
from mitzu.webapp.helper import MITZU_LOCATION, create_form_property_input
import traceback

EXTRA_PROPERTY_CONTAINER = "extra_property_container"
CONNECTION_DELETE_BUTTON = "connection_delete_button"
CONNECTION_TEST_BUTTON = "connection_test_button"
INDEX_TYPE = "connection_property"
TEST_CONNECTION_RESULT = "test_connection_result"
DELETE_CONNECTION_RESULT = "delete_connection_result"
TEST_CONNECTION_LOADING = "test_connection_loading"

CONFIRM_DIALOG_INDEX = "connection_confirm"
CONFIRM_DIALOG_CLOSE = "conn_confirm_dialog_close"
CONFIRM_DIALOG_ACCEPT = "conn_confirm_dialog_accept"


PROP_CONNECTION_ID = "connection_id"
PROP_CONNECTION_NAME = "connection_name"
PROP_CONNECTION_TYPE = "connection_type"
PROP_CATALOG = "catalog"
PROP_REGION = "region"
PROP_S3_STAGING_DIR = "s3_staging_dir"
PROP_WAREHOUSE = "warehouse"
PROP_ROLE = "role"
PROP_HTTP_PATH = "http_path"
PROP_PORT = "port"
PROP_HOST = "host"
PROP_USERNAME = "username"
PROP_PASSWORD = "password"
NOT_REQUIRED_PROPERTIES = [PROP_CATALOG, PROP_PORT, PROP_PASSWORD, PROP_USERNAME]
CON_TYPE_BLACKLIST = [M.ConnectionType.FILE, M.ConnectionType.SQLITE]


MIN_LENGTH = 4
MAX_LENGTH = 100


def create_url_param(values: Dict[str, Any], property: str) -> str:
    return f"{property}={values[property]}"


def create_connection_from_values(values: Dict[str, Any]) -> M.Connection:
    con_type = M.ConnectionType.parse(values[PROP_CONNECTION_TYPE])
    url_params = ""
    extra_configs = {}
    if con_type == M.ConnectionType.ATHENA:
        url_params = "&".join(
            [
                create_url_param(values, PROP_S3_STAGING_DIR),
                create_url_param(values, PROP_REGION),
            ]
        )
    elif con_type == M.ConnectionType.DATABRICKS:
        extra_configs = {PROP_HTTP_PATH: values[PROP_HTTP_PATH]}
    elif con_type == M.ConnectionType.SNOWFLAKE:
        url_params = "&".join(
            [
                create_url_param(values, PROP_WAREHOUSE),
                create_url_param(values, PROP_ROLE),
            ]
        )
    return M.Connection(
        connection_name=values[PROP_CONNECTION_NAME],
        connection_type=con_type,
        host=values[PROP_HOST],
        port=values[PROP_PORT],
        id=values[PROP_CONNECTION_ID],
        catalog=values[PROP_CATALOG],
        user_name=values[PROP_USERNAME],
        secret_resolver=M.ConstSecretResolver(values[PROP_PASSWORD]),
        url_params=url_params,
        extra_configs=extra_configs,
    )


def create_connection_extra_inputs(
    con_type: Optional[M.ConnectionType], con: Optional[M.Connection]
) -> List[bc.Component]:
    if con_type is None:
        return []
    if con_type == M.ConnectionType.ATHENA:
        return [
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_REGION,
                type="text",
                required=True,
                icon_cls="bi bi-globe",
                value=con.get_url_param(PROP_REGION) if con is not None else None,
                minlength=4,
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_S3_STAGING_DIR,
                type="text",
                required=True,
                icon_cls="bi bi-bucket",
                value=(
                    con.get_url_param(PROP_S3_STAGING_DIR) if con is not None else None
                ),
                minlength=4,
            ),
        ]
    if con_type == M.ConnectionType.DATABRICKS:
        return [
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_HTTP_PATH,
                type="text",
                required=True,
                icon_cls="bi bi-link",
                value=con.extra_configs.get(PROP_HTTP_PATH)
                if con is not None
                else None,
                minlength=4,
            )
        ]
    if con_type == M.ConnectionType.SNOWFLAKE:
        return [
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_WAREHOUSE,
                type="text",
                required=True,
                icon_cls="bi bi-house",
                value=con.get_url_param(PROP_WAREHOUSE) if con is not None else None,
                minlength=4,
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_ROLE,
                type="text",
                required=True,
                icon_cls="bi bi-file-earmark-person-fill",
                value=con.get_url_param(PROP_ROLE) if con is not None else None,
                minlength=4,
            ),
        ]

    return []


def create_delete_button(connection: Optional[M.Connection]) -> bc.Component:
    if connection is not None:
        return dbc.Button(
            [html.B(className="bi bi-x-circle"), " Delete connection"],
            id=CONNECTION_DELETE_BUTTON,
            color="danger",
        )
    else:
        return html.Div()


def create_confirm_dialog():
    return dbc.Modal(
        [
            dbc.ModalBody(
                "Do you really want to delete the connection?", class_name="lead"
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        "Close",
                        id=CONFIRM_DIALOG_CLOSE,
                        size="sm",
                        color="secondary",
                        class_name="me-1",
                    ),
                    dbc.Button(
                        "Delete",
                        id=CONFIRM_DIALOG_ACCEPT,
                        size="sm",
                        color="danger",
                        href=P.CONNECTIONS_PATH,
                        external_link=True,
                    ),
                ]
            ),
        ],
        id=CONFIRM_DIALOG_INDEX,
        is_open=False,
    )


def create_manage_connection_component(
    con: Optional[M.Connection],
) -> bc.Component:
    con_type_opts = [
        {"label": ct.name.title(), "value": ct.name}
        for ct in M.ConnectionType
        if ct not in CON_TYPE_BLACKLIST
    ]
    if CO.SETUP_SAMPLE_PROJECT:
        con_type_opts.append(
            {
                "label": M.ConnectionType.SQLITE.name.title(),
                "value": M.ConnectionType.SQLITE.name,
            }
        )
    def_con_type = M.ConnectionType.SNOWFLAKE
    return html.Div(
        [
            html.Div(
                create_form_property_input(
                    index_type=INDEX_TYPE,
                    property=PROP_CONNECTION_ID,
                    icon_cls="bi bi-info-circle",
                    type="text",
                    value=con.id if con is not None else str(uuid4())[-12:],
                    disabled=True,
                    class_name="d-none",
                ),
                className="d-none",
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_CONNECTION_NAME,
                icon_cls="bi bi-card-text",
                type="text",
                required=True,
                value=con.connection_name if con is not None else None,
                minlength=4,
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_CONNECTION_TYPE,
                icon_cls="bi bi-gear-wide",
                component_type=dmc.Select,
                required=True,
                value=(
                    con.connection_type.name if con is not None else def_con_type.name
                ),
                searchable=True,
                data=con_type_opts,
                size="xs",
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_HOST,
                icon_cls="bi bi-link",
                type="text",
                required=True,
                value=con.host if con is not None else None,
                minlength=4,
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_PORT,
                icon_cls="bi bi-hash",
                type="number",
                value=con.port if con is not None else None,
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_CATALOG,
                icon_cls="bi bi-journals",
                type="text",
                value=con.catalog if con is not None else None,
            ),
            html.Hr(),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_USERNAME,
                icon_cls="bi bi-person-fill",
                type="text",
                value=con.user_name if con is not None else None,
                minlength=4,
            ),
            create_form_property_input(
                index_type=INDEX_TYPE,
                property=PROP_PASSWORD,
                icon_cls="bi bi-lock-fill",
                type="password",
                value=con.password if con is not None else None,
                minlength=4,
            ),
            html.Hr(),
            html.Div(
                children=create_connection_extra_inputs(
                    con.connection_type if con is not None else def_con_type, con
                ),
                id=EXTRA_PROPERTY_CONTAINER,
            ),
            html.Hr(),
            html.Div(
                [
                    dbc.Button(
                        [html.B(className="bi bi-check-circle"), " Test connection"],
                        id=CONNECTION_TEST_BUTTON,
                        color="primary",
                        class_name="me-3",
                    ),
                    create_delete_button(con),
                ],
                className="mb-3",
            ),
            dbc.Button(
                [dbc.Spinner(size="sm"), " Loading..."],
                color="light",
                id=TEST_CONNECTION_LOADING,
                disabled=True,
                size="sm",
                class_name="border-1",
                style={"display": "none"},
            ),
            html.Div(children=[], id=TEST_CONNECTION_RESULT),
            html.Div(children=[], id=DELETE_CONNECTION_RESULT),
            create_confirm_dialog(),
        ],
    )


@callback(
    Output(EXTRA_PROPERTY_CONTAINER, "children"),
    Input({"type": INDEX_TYPE, "index": PROP_CONNECTION_TYPE}, "value"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def connection_type_changed(connection_type: str, pathname: str) -> List[bc.Component]:
    deps: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    if pathname == P.CONNECTIONS_CREATE_PATH:
        connection: Optional[M.Connection] = None
    else:
        connection_id = P.get_path_value(
            P.CONNECTIONS_MANAGE_PATH, pathname, P.CONNECTION_ID_PATH_PART
        )
        connection = deps.storage.get_connection(connection_id)
    con_type = M.ConnectionType.parse(connection_type)
    return create_connection_extra_inputs(con_type, connection)


@callback(
    Output(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    Input(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def delete_confirmed_clicked(n_clicks: int, pathname: str) -> int:
    if n_clicks is None:
        return no_update
    deps: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    connection_id = P.get_path_value(
        P.CONNECTIONS_MANAGE_PATH, pathname, P.CONNECTION_ID_PATH_PART
    )
    deps.storage.delete_connection(connection_id)
    return n_clicks


@callback(
    Output(CONFIRM_DIALOG_INDEX, "is_open"),
    Output(DELETE_CONNECTION_RESULT, "children"),
    Input(CONNECTION_DELETE_BUTTON, "n_clicks"),
    Input(CONFIRM_DIALOG_CLOSE, "n_clicks"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def delete_button_clicked(delete: int, close: int, pathname: str) -> Tuple[bool, str]:
    if delete is None:
        return no_update, no_update
    deps: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    if ctx.triggered_id == CONNECTION_DELETE_BUTTON:
        connection_id = P.get_path_value(
            P.CONNECTIONS_MANAGE_PATH, pathname, P.CONNECTION_ID_PATH_PART
        )
        project_ids = deps.storage.list_projects()
        for p_id in project_ids:
            prj = deps.storage.get_project(p_id)
            if prj.get_connection_id() == connection_id:
                return False, html.Div(
                    children=[
                        html.Span(
                            "You can't delete this connection because it is used by  "
                        ),
                        dcc.Link(
                            prj.project_name,
                            P.create_path(P.PROJECTS_MANAGE_PATH, project_id=p_id),
                        ),
                    ],
                    className="my-3 text-danger lead",
                )
        return True, ""

    return False, ""


def validate_input_values(values: Dict[str, Any]) -> Optional[str]:
    for k, v in values.items():
        if k not in NOT_REQUIRED_PROPERTIES and (
            v is None or len(v) < MIN_LENGTH or len(v) > MAX_LENGTH
        ):
            return k
    return None


@callback(
    Output(TEST_CONNECTION_RESULT, "children"),
    Input(CONNECTION_TEST_BUTTON, "n_clicks"),
    State({"type": INDEX_TYPE, "index": ALL}, "value"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
    background=True,
    running=[
        (
            Output(TEST_CONNECTION_LOADING, "style"),
            {"display": "inline-block"},
            {"display": "none"},
        ),
    ],
    interval=100,
)
@restricted
def test_connection_clicked(
    n_clicks: int, values: Dict[str, Any], pathname: str
) -> List[bc.Component]:
    if n_clicks is None:
        return no_update

    vals = {}
    for prop in ctx.args_grouping[1]:
        id_val = prop["id"]
        if id_val.get("type") == INDEX_TYPE:
            vals[id_val.get("index")] = prop["value"]

    invalid = validate_input_values(values=vals)
    if invalid is not None:
        return html.P(f"Invalid {value_to_label(invalid)}", className="lead my-3")

    try:
        connection = create_connection_from_values(vals)
        dummy_project = M.Project(connection, [], project_name="dp")
        dummy_project.get_adapter().test_connection()
        return html.P("Connected successfully!", className="lead my-3")
    except Exception as exc:
        traceback.print_exc()
        return html.P(f"Failed to connect: {exc}", className="my-3 text-danger")
