from typing import Any, Dict, List, Optional

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from dash import html, no_update
from mitzu.helper import create_unique_id
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.paths as P
from mitzu.webapp.helper import create_form_property_input
import traceback
import json
import mitzu.adapters.trino_adapter as TA
from dataclasses import dataclass

EXTRA_PROPERTY_CONTAINER = "extra_property_container"
CONNECTION_TEST_BUTTON = "connection_test_button"
TEST_CONNECTION_RESULT = "test_connection_result"
TEST_CONNECTION_LOADING = "test_connection_loading"


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
PROP_BIGQUERY_CREDENTIALS = "credentials"


CON_TYPE_BLACKLIST = [M.ConnectionType.FILE]


@dataclass(frozen=True)
class ConnectionComponentConfig:
    index_type: str
    test_loading_id: str
    label_sm: int = 12
    input_sm: int = 12
    label_lg: int = 3
    input_lg: int = 3


def create_url_param(values: Dict[str, Any], property: str) -> str:
    return f"{property}={values[property]}"


def validate_inputs(values: Dict[str, Any]):
    if values.get(PROP_CONNECTION_NAME) is None:
        raise ValueError("Connection name can't be empty")


def create_connection_from_values(values: Dict[str, Any]) -> M.Connection:
    deps = DEPS.Dependencies.get()
    connection_id = values[PROP_CONNECTION_ID]
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
        region = values[PROP_REGION]
        values[PROP_HOST] = f"athena.{region}.amazonaws.com"
    elif con_type == M.ConnectionType.DATABRICKS:
        extra_configs = {PROP_HTTP_PATH: values[PROP_HTTP_PATH]}
        values[PROP_USERNAME] = "token"
    elif con_type == M.ConnectionType.SNOWFLAKE:
        url_params = "&".join(
            [
                create_url_param(values, PROP_WAREHOUSE),
                create_url_param(values, PROP_ROLE),
            ]
        )
    elif con_type == M.ConnectionType.TRINO:
        extra_configs[TA.ROLE_EXTRA_CONFIG] = values[PROP_ROLE]
        if values.get(PROP_PORT) is None:
            values[PROP_PORT] = 443
    elif con_type == M.ConnectionType.BIGQUERY:
        creds = json.loads(values[PROP_BIGQUERY_CREDENTIALS])
        extra_configs = {PROP_BIGQUERY_CREDENTIALS: creds}

    elif con_type == M.ConnectionType.REDSHIFT:
        if values.get(PROP_PORT) is None:
            values[PROP_PORT] = 5439

    validate_inputs(values)

    return M.Connection(
        connection_name=values[PROP_CONNECTION_NAME],
        connection_type=con_type,
        host=values[PROP_HOST],
        port=values.get(PROP_PORT),
        id=connection_id,
        catalog=values.get(PROP_CATALOG),
        user_name=values.get(PROP_USERNAME),
        secret_resolver=deps.secret_service.get_secret_resolver(
            values.get(PROP_PASSWORD, None)
        ),
        url_params=url_params,
        extra_configs=extra_configs,
    )


def create_athena_connection_inputs(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig
):
    return [
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_USERNAME,
            icon_cls="bi bi-person-fill",
            type="text",
            label="AWS Access Key ID",
            value=con.user_name if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PASSWORD,
            icon_cls="bi bi-lock-fill",
            type="password",
            label="AWS Secret Access Key",
            value=con.password if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_CATALOG,
            icon_cls="bi bi-journals",
            type="text",
            value=con.catalog if con is not None else None,
            placeholder="AwsDataCatalog",
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_REGION,
            type="text",
            icon_cls="bi bi-globe",
            label="AWS Region",
            value=con.get_url_param(PROP_REGION) if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_S3_STAGING_DIR,
            type="text",
            required=True,
            icon_cls="bi bi-bucket",
            value=(con.get_url_param(PROP_S3_STAGING_DIR) if con is not None else None),
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
    ]


def create_databricks_connnection_inputs(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig
):
    return [
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_HOST,
            icon_cls="bi bi-link",
            type="text",
            required=True,
            value=con.host if con is not None else None,
            placeholder="workspace.cloud.databricks.com",
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PORT,
            icon_cls="bi bi-hash",
            placeholder=443,
            type="number",
            value=con.port if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_CATALOG,
            icon_cls="bi bi-journals",
            type="text",
            value=con.catalog if con is not None else None,
            placeholder="hive_metastore",
            label="Databricks Catalog",
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PASSWORD,
            icon_cls="bi bi-lock-fill",
            type="password",
            label="Personal Access Token",
            required=True,
            value=con.password if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_HTTP_PATH,
            type="text",
            required=True,
            icon_cls="bi bi-link",
            value=con.extra_configs.get(PROP_HTTP_PATH) if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
    ]


def create_snowflake_connection_input(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig
):
    return [
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_HOST,
            icon_cls="bi bi-link",
            label="Cluster",
            type="text",
            required=True,
            value=con.host if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_CATALOG,
            icon_cls="bi bi-journals",
            type="text",
            required=True,
            value=con.catalog if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_USERNAME,
            icon_cls="bi bi-person-fill",
            type="text",
            value=con.user_name if con is not None else None,
            required=True,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PASSWORD,
            icon_cls="bi bi-lock-fill",
            type="password",
            label="Cluster Password",
            required=True,
            value=con.password if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_WAREHOUSE,
            type="text",
            required=True,
            icon_cls="bi bi-house",
            value=con.get_url_param(PROP_WAREHOUSE) if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_ROLE,
            type="text",
            required=True,
            icon_cls="bi bi-file-earmark-person-fill",
            value=con.get_url_param(PROP_ROLE) if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
    ]


def create_basic_connection_input(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig, def_port: int
):
    return [
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_HOST,
            icon_cls="bi bi-link",
            type="text",
            required=True,
            value=con.host if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PORT,
            icon_cls="bi bi-hash",
            placeholder=def_port,
            type="number",
            value=con.port if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_CATALOG,
            label="Database",
            icon_cls="bi bi-journals",
            type="text",
            value=con.catalog if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_USERNAME,
            icon_cls="bi bi-person-fill",
            type="text",
            value=con.user_name if con is not None else None,
            required=True,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PASSWORD,
            icon_cls="bi bi-lock-fill",
            type="password",
            required=True,
            value=con.password if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
    ]


def create_sqlite_connection_input(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig
):
    return [
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_HOST,
            icon_cls="bi bi-link",
            type="text",
            required=True,
            value=con.host if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        )
    ]


def create_trino_connection_input(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig
):
    return [
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_HOST,
            icon_cls="bi bi-link",
            type="text",
            required=True,
            value=con.host if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PORT,
            icon_cls="bi bi-hash",
            placeholder=443,
            type="number",
            value=con.port if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_CATALOG,
            icon_cls="bi bi-journals",
            type="text",
            required=True,
            value=con.catalog if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_USERNAME,
            icon_cls="bi bi-person-fill",
            type="text",
            value=con.user_name if con is not None else None,
            required=True,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_PASSWORD,
            icon_cls="bi bi-lock-fill",
            type="password",
            required=True,
            value=con.password if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_ROLE,
            type="text",
            required=True,
            icon_cls="bi bi-file-earmark-person-fill",
            value=con.extra_configs.get(PROP_ROLE) if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
    ]


def create_bigquery_connection_input(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig
):
    return [
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_HOST,
            icon_cls="bi bi-link",
            label="Bigquery Project",
            type="text",
            required=True,
            value=con.host if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_CATALOG,
            icon_cls="bi bi-journals",
            type="text",
            label="Default Dataset",
            required=True,
            value=con.catalog if con is not None else None,
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
        create_form_property_input(
            index_type=cc_config.index_type,
            property=PROP_BIGQUERY_CREDENTIALS,
            icon_cls="bi bi-key me-1",
            component_type=dbc.Input,
            type="password",
            label="Credentials JSON",
            required=True,
            value=(
                json.dumps(con.extra_configs.get(PROP_BIGQUERY_CREDENTIALS))
                if con is not None
                else None
            ),
            input_lg=cc_config.input_lg,
            label_lg=cc_config.label_lg,
            input_sm=cc_config.input_sm,
            label_sm=cc_config.label_sm,
        ),
    ]


def create_connection_extra_inputs(
    con_type: Optional[M.ConnectionType],
    con: Optional[M.Connection],
    cc_config: ConnectionComponentConfig,
) -> List[bc.Component]:
    if con_type is None:
        return []

    if con_type == M.ConnectionType.ATHENA:
        return create_athena_connection_inputs(con, cc_config)
    elif con_type == M.ConnectionType.DATABRICKS:
        return create_databricks_connnection_inputs(con, cc_config)
    elif con_type == M.ConnectionType.SNOWFLAKE:
        return create_snowflake_connection_input(con, cc_config)
    elif con_type == M.ConnectionType.POSTGRESQL:
        return create_basic_connection_input(con, cc_config, def_port=5432)
    elif con_type == M.ConnectionType.MYSQL:
        return create_basic_connection_input(con, cc_config, def_port=3306)
    elif con_type == M.ConnectionType.REDSHIFT:
        return create_basic_connection_input(con, cc_config, def_port=5439)
    elif con_type == M.ConnectionType.TRINO:
        return create_trino_connection_input(con, cc_config)
    elif con_type == M.ConnectionType.BIGQUERY:
        return create_bigquery_connection_input(con, cc_config)
    elif con_type == M.ConnectionType.SQLITE:
        return create_sqlite_connection_input(con, cc_config)
    else:
        return html.Div("Unsupported connection type", className="lead text-warning")


def create_manage_connection_component(
    con: Optional[M.Connection], cc_config: ConnectionComponentConfig
) -> bc.Component:
    con_type_opts = [
        {"label": ct.name.title(), "value": ct.name}
        for ct in M.ConnectionType
        if ct not in CON_TYPE_BLACKLIST
    ]
    def_con_type = M.ConnectionType.SNOWFLAKE
    return html.Div(
        [
            html.Div(
                create_form_property_input(
                    index_type=cc_config.index_type,
                    property=PROP_CONNECTION_ID,
                    icon_cls="bi bi-info-circle",
                    type="text",
                    value=con.id if con is not None else create_unique_id(),
                    disabled=True,
                    class_name="d-none",
                ),
                className="d-none",
            ),
            create_form_property_input(
                index_type=cc_config.index_type,
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
                input_lg=cc_config.input_lg,
                label_lg=cc_config.label_lg,
                input_sm=cc_config.input_sm,
                label_sm=cc_config.label_sm,
            ),
            create_form_property_input(
                index_type=cc_config.index_type,
                property=PROP_CONNECTION_NAME,
                icon_cls="bi bi-card-text",
                type="text",
                required=True,
                value=con.connection_name if con is not None else "New connection",
                input_lg=cc_config.input_lg,
                label_lg=cc_config.label_lg,
                input_sm=cc_config.input_sm,
                label_sm=cc_config.label_sm,
            ),
            html.Hr(),
            html.Div(
                children=create_connection_extra_inputs(
                    con.connection_type if con is not None else def_con_type,
                    con,
                    cc_config,
                ),
                id={"index": EXTRA_PROPERTY_CONTAINER, "type": cc_config.index_type},
            ),
            html.Hr(),
            html.Div(
                [
                    dbc.Button(
                        [html.B(className="bi bi-check-circle"), " Test connection"],
                        id={
                            "index": CONNECTION_TEST_BUTTON,
                            "type": cc_config.index_type,
                        },
                        color="primary",
                        class_name="me-3",
                        size="sm",
                    ),
                ],
                className="mb-3",
            ),
            dbc.Button(
                [dbc.Spinner(size="sm"), " Loading..."],
                color="light",
                id=cc_config.test_loading_id,
                disabled=True,
                size="sm",
                class_name="border-1",
                style={"display": "none"},
            ),
            html.Div(
                children=[],
                id={"index": TEST_CONNECTION_RESULT, "type": cc_config.index_type},
            ),
        ],
    )


def connection_type_changed(
    connection_type: str, pathname: str, cc_config: ConnectionComponentConfig
) -> List[bc.Component]:
    deps = DEPS.Dependencies.get()
    connection: Optional[M.Connection] = None
    if pathname.startswith(P.CONNECTIONS_MANAGE_PATH):
        connection_id = P.get_path_value(
            P.CONNECTIONS_MANAGE_PATH, pathname, P.CONNECTION_ID_PATH_PART
        )
        connection = deps.storage.get_connection(connection_id)

    con_type = M.ConnectionType.parse(connection_type)
    return create_connection_extra_inputs(con_type, connection, cc_config)


def create_connection_from_inputs(
    cc_config: ConnectionComponentConfig, args_groupping: List
) -> M.Connection:
    vals = {}
    for prop in args_groupping:
        id_val = prop["id"]
        if id_val.get("type") == cc_config.index_type:
            vals[id_val.get("index")] = prop["value"]
    return create_connection_from_values(vals)


def test_connection_clicked(
    n_clicks: int,
    cc_config: ConnectionComponentConfig,
    args_groupping: List,
) -> List[bc.Component]:
    if n_clicks is None:
        return no_update
    try:
        connection = create_connection_from_inputs(cc_config, args_groupping)
        dummy_project = M.Project(connection, [], project_name="dp")
        dummy_project.get_adapter().test_connection()
        return html.P("Connected successfully!", className="lead my-3")
    except Exception as exc:
        traceback.print_exc()
        return html.P(f"Failed to connect: {exc}", className="my-3 text-danger")
