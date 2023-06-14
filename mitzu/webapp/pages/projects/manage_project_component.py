import dash_bootstrap_components as dbc
from dash import Input, Output, callback, html, ctx, no_update, State, ALL
import dash.development.base_component as bc
from typing import Optional, Dict, List, Any
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S
from mitzu.webapp.auth.decorator import restricted
from mitzu.webapp.helper import create_form_property_input, MITZU_LOCATION
from mitzu.helper import value_to_label, create_unique_id
from mitzu.webapp.pages.projects.helper import PROP_CONNECTION, PROJECT_INDEX_TYPE
import mitzu.webapp.pages.projects.event_tables_config as ETC
import dash_mantine_components as dmc
import mitzu.webapp.pages.paths as P
from uuid import uuid4
import mitzu.webapp.pages.connections.manage_connections_component as MCC
import traceback


CREATE_PROJECT_DOCS_LINK = "https://github.com/mitzu-io/mitzu/blob/main/DOCS.md"
PROJECT_DETAILS_CONTAINER = "project-details-container"


PROP_PROJECT_ID = "project_id"
PROP_PROJECT_NAME = "project_name"
PROP_DESCRIPTION = "description"

PROP_DISC_LOOKBACK_DAYS = "lookback_days"
PROP_DISC_SAMPLE_SIZE = "sample_size"

PROP_EXPLORE_AUTO_REFRESH = "auto_refresh"
PROP_END_DATE_CONFIG = "default_end_date_config"
PROP_CUSTOM_END_DATE_CONFIG = "custom_default_end_date"

NEW_CONNECTION_BUTTON = "new_connection_button"
EDIT_CONNECTION_BUTTON = "edit_connection_button"
SAVE_CONNECTION_BUTTON = "save_connection_button"
CLOSE_CONNECTION_BUTTON = "close_connection_button"
SAVE_CONNECTIONS_INFO = "save_connections_info"
EDIT_CONNECTION_MODAL = "edit_conncetion_modal"

EDIT_PAGE_CON_INDEX_TYPE = "edit_page_con_index_type"
EDIT_PAGE_CON_LOADING = "edit_page_con_test_loading"

EDIT_PAGE_CC_CONIFG = MCC.ConnectionComponentConfig(
    index_type=EDIT_PAGE_CON_INDEX_TYPE,
    test_loading_id=EDIT_PAGE_CON_LOADING,
    input_lg=8,
    input_sm=8,
    label_lg=4,
    label_sm=4,
)


ADVANCED_SETTINGS_LINK = "advanced_settigns"
ADVANCED_SETTINGS_BODY = "advanced_settigns_body"


def create_edit_connection_modal():
    return dbc.Modal(
        id=EDIT_CONNECTION_MODAL,
        children=[
            dbc.ModalHeader(dbc.ModalTitle("Establish connection"), close_button=False),
            dbc.ModalBody(
                children=[
                    MCC.create_manage_connection_component(
                        con=None, cc_config=EDIT_PAGE_CC_CONIFG
                    ),
                    html.Div(
                        children=[],
                        id=SAVE_CONNECTIONS_INFO,
                        className="lead text-danger",
                    ),
                ]
            ),
            dbc.ModalFooter(
                children=[
                    dbc.Button(
                        children=[html.I(className="bi bi-x me-1"), "Close"],
                        color="secondary",
                        size="sm",
                        id=CLOSE_CONNECTION_BUTTON,
                    ),
                    dbc.Button(
                        children=[html.I(className="bi bi-plus-circle me-1"), "Save"],
                        color="success",
                        size="sm",
                        id=SAVE_CONNECTION_BUTTON,
                    ),
                ]
            ),
        ],
        is_open=False,
    )


def create_project_settings(
    project: Optional[M.Project], dependencies: DEPS.Dependencies, **query_args
) -> bc.Component:
    return html.Div(
        [
            dbc.Form(
                children=[
                    dbc.Card(
                        children=[
                            dbc.CardHeader(
                                html.P(
                                    [
                                        html.I(className="bi bi-gear me-1"),
                                        "Project setup",
                                    ],
                                    className="lead",
                                ),
                            ),
                            dbc.CardBody(
                                create_basic_project_settings(
                                    project, dependencies, **query_args
                                )
                            ),
                        ],
                        class_name="mb-3",
                    ),
                    dbc.Card(
                        children=[
                            dbc.CardHeader(
                                [
                                    html.P(
                                        children=[
                                            "Event tables",
                                            html.I(
                                                className="bi bi-question-circle ms-1",
                                                id="event_table_help",
                                            ),
                                        ],
                                        className="lead",
                                    ),
                                    dbc.Popover(
                                        children=[
                                            """Event table are tables in the data warehouse that contain user events. 
                The user id and event time columns are mandatory for the event tables. 
                The event name column is optional and reserved for 
                data warehouse tables that contain multiple event types.""",
                                        ],
                                        target="event_table_help",
                                        body=True,
                                        trigger="hover",
                                    ),
                                ]
                            ),
                            dbc.CardBody(
                                ETC.create_event_tables(project),
                            ),
                        ],
                        class_name="mb-3",
                    ),
                    dbc.Card(
                        children=[
                            dbc.CardHeader(
                                html.A(
                                    children=[
                                        "Advanced settings",
                                        html.I(className="bi bi-plus-circle ms-1"),
                                    ],
                                    id=ADVANCED_SETTINGS_LINK,
                                    className="lead text-body",
                                ),
                            ),
                            dbc.CardBody(
                                children=[
                                    create_explore_settings(project),
                                    create_discovery_settings(project),
                                ],
                                class_name="d-none",
                                id=ADVANCED_SETTINGS_BODY,
                            ),
                        ],
                        class_name="mb-3",
                    ),
                ],
                id=PROJECT_DETAILS_CONTAINER,
                class_name="mt-3 ",
            ),
        ],
    )


def get_connection_options(storage: S.MitzuStorage) -> List:
    con_ids = storage.list_connections()
    connections = [storage.get_connection(c_id) for c_id in con_ids]
    return [{"label": c.connection_name, "value": c.id} for c in connections]


def create_basic_project_settings(
    project: Optional[M.Project], dependencies: DEPS.Dependencies, **query_args
) -> bc.Component:
    if project is not None:
        project_name = project.project_name
        connection_id = project.connection.id
    else:
        project_name_id = len(dependencies.storage.list_projects()) + 1
        project_name = f"Project ({project_name_id})"
        connection_id = None

    if connection_id is None:
        connection_id = query_args.get("connection_id", None)

    con_options = get_connection_options(dependencies.storage)

    return html.Div(
        [
            html.Div(
                create_form_property_input(
                    property=PROP_PROJECT_ID,
                    index_type=PROJECT_INDEX_TYPE,
                    icon_cls="bi bi-info-circle",
                    value=(project.id if project is not None else str(uuid4())[-12:]),
                    disabled=True,
                ),
                className="d-none",
            ),
            create_form_property_input(
                property=PROP_PROJECT_NAME,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-card-text",
                value=project_name,
                required=True,
                minlength=4,
                maxlength=100,
            ),
            create_form_property_input(
                property=PROP_CONNECTION,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-link",
                component_type=dmc.Select,
                value=(connection_id),
                data=con_options,
                size="xs",
                required=True,
                placeholder="Select connection",
                clearable=True,
            ),
            dbc.Row(
                [
                    dbc.Col("", lg=3, sm=12, class_name="m-0"),
                    dbc.Col(
                        [
                            dbc.Button(
                                [
                                    html.B(className="bi bi-plus-circle me-1"),
                                    "Add connection",
                                ],
                                id=NEW_CONNECTION_BUTTON,
                                color="secondary",
                                class_name="mb-3 me-3",
                                size="sm",
                            ),
                            dbc.Button(
                                [
                                    html.B(className="bi bi-gear me-1"),
                                    "Manage",
                                ],
                                id=EDIT_CONNECTION_BUTTON,
                                color="secondary",
                                class_name="mb-3",
                                size="sm",
                                href=(
                                    P.create_path(
                                        P.CONNECTIONS_MANAGE_PATH,
                                        connection_id=connection_id,
                                    )
                                    if connection_id is not None
                                    else ""
                                ),
                                disabled=(connection_id is None),
                            ),
                        ],
                        lg=3,
                        sm=12,
                    ),
                ]
            ),
            create_form_property_input(
                property=PROP_DESCRIPTION,
                value=project.description if project else None,
                index_type=PROJECT_INDEX_TYPE,
                component_type=dbc.Textarea,
                icon_cls="bi bi-blockquote-left",
                placeholder="Project description",
                rows=4,
                maxlength=300,
            ),
            create_edit_connection_modal(),
        ],
    )


def create_discovery_settings(project: Optional[M.Project]) -> bc.Component:
    if project is not None:
        disc_settings = project.discovery_settings
    else:
        disc_settings = M.DiscoverySettings()
    return html.Div(
        [
            html.P(
                children=[
                    "Discovery settings",
                    html.I(
                        className="bi bi-question-circle ms-1",
                        id="discovery_settings_help",
                    ),
                ],
                className="mb-3 lead",
            ),
            dbc.Popover(
                children=[
                    "Discovery settings change how Mitzu understands the connected data warehouse."
                ],
                target="discovery_settings_help",
                body=True,
                trigger="hover",
            ),
            create_form_property_input(
                property=PROP_DISC_LOOKBACK_DAYS,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-clock-history",
                value=disc_settings.lookback_days,
                required=True,
                type="number",
                min=1,
            ),
            create_form_property_input(
                property=PROP_DISC_SAMPLE_SIZE,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-filter-square",
                value=disc_settings.min_property_sample_size,
                required=True,
                type="number",
                min=1,
            ),
        ]
    )


def create_explore_settings(project: Optional[M.Project]) -> bc.Component:
    if project is not None:
        webapp_settings = project.webapp_settings
    else:
        webapp_settings = M.WebappSettings()

    return html.Div(
        [
            html.P(
                children=[
                    "Explore settings",
                    html.I(
                        className="bi bi-question-circle ms-1",
                        id="explore_settings_help",
                    ),
                ],
                className="mb-3 lead",
            ),
            dbc.Popover(
                children=[
                    "Explore settings change the default behaviour of the explore page.",
                ],
                target="explore_settings_help",
                body=True,
                trigger="hover",
            ),
            create_form_property_input(
                property=PROP_EXPLORE_AUTO_REFRESH,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-arrow-clockwise",
                value=webapp_settings.auto_refresh_enabled,
                component_type=dbc.Checkbox,
            ),
            create_form_property_input(
                property=PROP_END_DATE_CONFIG,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-calendar2-check",
                value=webapp_settings.end_date_config.name.upper(),
                component_type=dmc.Select,
                data=[
                    {"label": value_to_label(v.name), "value": v.name.upper()}
                    for v in M.WebappEndDateConfig
                ],
                size="xs",
            ),
            create_form_property_input(
                property=PROP_CUSTOM_END_DATE_CONFIG,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-calendar3",
                inputFormat="YYYY-MM-DD",
                value=webapp_settings.custom_end_date,
                disabled=(
                    webapp_settings.end_date_config is None
                    or webapp_settings.end_date_config
                    != M.WebappEndDateConfig.CUSTOM_DATE
                ),
                component_type=dmc.DatePicker,
                size="xs",
            ),
        ]
    )


@callback(
    Output(
        {"type": PROJECT_INDEX_TYPE, "index": PROP_CUSTOM_END_DATE_CONFIG}, "disabled"
    ),
    Input({"type": PROJECT_INDEX_TYPE, "index": PROP_END_DATE_CONFIG}, "value"),
    prevent_initial_call=True,
)
@restricted
def end_date_config_changed(config_value: str):
    return config_value.upper() != M.WebappEndDateConfig.CUSTOM_DATE.name.upper()


@callback(
    Output(EDIT_CONNECTION_BUTTON, "disabled"),
    Output(EDIT_CONNECTION_BUTTON, "href"),
    Input({"type": PROJECT_INDEX_TYPE, "index": PROP_CONNECTION}, "value"),
    prevent_initial_call=True,
)
@restricted
def edit_connection_button_handler(value: str):
    return (
        value is None,
        ""
        if value is None
        else P.create_path(P.CONNECTIONS_MANAGE_PATH, connection_id=value),
    )


@callback(
    Output(ADVANCED_SETTINGS_BODY, "class_name"),
    Input(ADVANCED_SETTINGS_LINK, "n_clicks"),
    State(ADVANCED_SETTINGS_BODY, "class_name"),
    prevent_initial_call=True,
)
@restricted
def advanced_settings_clicekd(value: int, class_name: str):
    return "d-block" if class_name == "d-none" else "d-none"


@callback(
    Output(EDIT_CONNECTION_MODAL, "is_open"),
    Output({"type": PROJECT_INDEX_TYPE, "index": PROP_CONNECTION}, "value"),
    Output({"type": PROJECT_INDEX_TYPE, "index": PROP_CONNECTION}, "data"),
    Output(SAVE_CONNECTIONS_INFO, "children"),
    Output(
        {"type": EDIT_PAGE_CON_INDEX_TYPE, "index": MCC.PROP_CONNECTION_ID}, "value"
    ),
    Input(CLOSE_CONNECTION_BUTTON, "n_clicks"),
    Input(SAVE_CONNECTION_BUTTON, "n_clicks"),
    Input(NEW_CONNECTION_BUTTON, "n_clicks"),
    State({"type": EDIT_PAGE_CON_INDEX_TYPE, "index": ALL}, "value"),
    prevent_initial_call=True,
)
@restricted
def modal_open_handler(close: int, save: int, new: int, inputs: Dict):
    if ctx.triggered_id == CLOSE_CONNECTION_BUTTON:
        return False, no_update, no_update, "", no_update

    if ctx.triggered_id == SAVE_CONNECTION_BUTTON:
        try:
            deps = DEPS.Dependencies.get()
            con = MCC.create_connection_from_inputs(
                EDIT_PAGE_CC_CONIFG, ctx.args_grouping[3]
            )
            storage = deps.storage
            storage.set_connection(connection_id=con.id, connection=con)
            deps.tracking_service.track_connection_saved(con)
            data = get_connection_options(storage)
            return (False, con.id, data, "", no_update)
        except Exception as exc:
            traceback.print_exc()
            return (
                no_update,
                no_update,
                no_update,
                f"Failed to save connection: {str(exc)[:100]}...",
                no_update,
            )

    return True, no_update, no_update, "", create_unique_id()


@callback(
    Output(
        {"type": EDIT_PAGE_CON_INDEX_TYPE, "index": MCC.EXTRA_PROPERTY_CONTAINER},
        "children",
    ),
    Input(
        {"type": EDIT_PAGE_CON_INDEX_TYPE, "index": MCC.PROP_CONNECTION_TYPE}, "value"
    ),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
@restricted
def connection_type_changed_callback(
    connection_type: str, pathname: str
) -> List[bc.Component]:
    return MCC.connection_type_changed(
        connection_type=connection_type,
        pathname=pathname,
        cc_config=EDIT_PAGE_CC_CONIFG,
    )


@callback(
    Output(
        {"type": EDIT_PAGE_CON_INDEX_TYPE, "index": MCC.TEST_CONNECTION_RESULT},
        "children",
    ),
    Input(
        {"type": EDIT_PAGE_CON_INDEX_TYPE, "index": MCC.CONNECTION_TEST_BUTTON},
        "n_clicks",
    ),
    State({"type": EDIT_PAGE_CON_INDEX_TYPE, "index": ALL}, "value"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
    background=True,
    running=[
        (
            Output(
                EDIT_PAGE_CON_LOADING,
                "style",
            ),
            {"display": "inline-block"},
            {"display": "none"},
        ),
    ],
    interval=100,
)
@restricted
def test_connection_clicked_callback(
    n_clicks: int, values: Dict[str, Any], pathname: str
) -> List[bc.Component]:
    return MCC.test_connection_clicked(
        n_clicks=n_clicks,
        cc_config=EDIT_PAGE_CC_CONIFG,
        args_groupping=ctx.args_grouping[1],
    )
