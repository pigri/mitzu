import dash_bootstrap_components as dbc
from dash import Input, Output, callback, ctx, html, no_update, State
import dash.development.base_component as bc
from typing import Optional, cast
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import flask
import mitzu.webapp.pages.paths as P
from mitzu.webapp.helper import create_form_property_input, MITZU_LOCATION
from mitzu.webapp.pages.projects.helper import PROP_CONNECTION, PROJECT_INDEX_TYPE
import traceback
import dash_mantine_components as dmc
from uuid import uuid4

CREATE_PROJECT_DOCS_LINK = "https://github.com/mitzu-io/mitzu/blob/main/DOCS.md"
PROJECT_DETAILS_CONTAINER = "project-details-container"

DELETE_BUTTON = "project_delete_button"

PROP_PROJECT_ID = "project_id"
PROP_PROJECT_NAME = "project_name"
PROP_DESCRIPTION = "description"

PROP_DISC_LOOKBACK_DAYS = "lookback_days"
PROP_DISC_SAMPLE_SIZE = "sample_size"

PROP_EXPLORE_AUTO_REFRESH = "auto_refresh"

CONFIRM_DIALOG_INDEX = "project_delete_confirm"
CONFIRM_DIALOG_CLOSE = "project_delete_confirm_dialog_close"
CONFIRM_DIALOG_ACCEPT = "project_delete_confirm_dialog_accept"


def create_project_settings(
    project: Optional[M.Project], dependencies: DEPS.Dependencies
) -> bc.Component:
    return html.Div(
        [
            dbc.Form(
                children=[
                    html.P(
                        "Project settings explenation...",
                        className="mb-3 h6",
                    ),
                    create_basic_project_settings(project, dependencies),
                    html.Hr(),
                    create_discovery_settings(project),
                    html.Hr(),
                    create_explore_settings(project),
                    html.Hr(),
                    create_delete_button(project),
                    create_discovery_button(project),
                ],
                id=PROJECT_DETAILS_CONTAINER,
                class_name="mt-3",
            ),
            create_confirm_dialog(project),
        ],
    )


def create_delete_button(project: Optional[M.Project]) -> bc.Component:
    if project is not None:
        return dbc.Button(
            [html.B(className="bi bi-x-circle me-1"), "Delete Project"],
            id=DELETE_BUTTON,
            color="danger",
            class_name="d-inline-block me-3  mb-3",
        )
    else:
        return html.Div()


def create_discovery_button(project: Optional[M.Project]) -> bc.Component:
    if project is not None:
        return dbc.Button(
            [html.B(className="bi bi-search me-1"), "Discover Project"],
            color="primary",
            class_name="d-inline-block me-3 mb-3",
            href=P.create_path(
                P.EVENTS_AND_PROPERTIES_PROJECT_PATH, project_id=project.id
            ),
        )
    else:
        return html.Div()


def create_basic_project_settings(
    project: Optional[M.Project], dependencies: DEPS.Dependencies
) -> bc.Component:
    if project is not None:
        project_name = project.project_name
    else:
        project_name = None

    con_ids = dependencies.storage.list_connections()
    connections = [dependencies.storage.get_connection(c_id) for c_id in con_ids]
    con_options = [{"label": c.connection_name, "value": c.id} for c in connections]

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
                value=(project.connection.id if project is not None else None),
                data=con_options,
                size="xs",
                required=True,
            ),
            dbc.Row(
                [
                    dbc.Col("", lg=3, sm=12, class_name="m-0"),
                    dbc.Col(
                        dbc.Button(
                            [
                                html.B(className="bi bi-plus-circle me-1"),
                                "New connection",
                            ],
                            href=P.CONNECTIONS_CREATE_PATH,
                            color="primary",
                            class_name="mb-3",
                            size="sm",
                        ),
                        lg=3,
                        sm=12,
                    ),
                ]
            ),
            create_form_property_input(
                property=PROP_DESCRIPTION,
                index_type=PROJECT_INDEX_TYPE,
                component_type=dbc.Textarea,
                icon_cls="bi bi-blockquote-left",
                rows=4,
                maxlength=300,
            ),
        ],
    )


def create_confirm_dialog(project: Optional[M.Project]):
    if project is None:
        return html.Div()
    return dbc.Modal(
        [
            dbc.ModalBody(
                f"Do you really want to delete the {project.project_name}?",
                class_name="lead",
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
                        href=P.PROJECTS_PATH,
                        external_link=True,
                    ),
                ]
            ),
        ],
        id=CONFIRM_DIALOG_INDEX,
        is_open=False,
    )


def create_discovery_settings(project: Optional[M.Project]) -> bc.Component:
    if project is not None:
        disc_settings = project.discovery_settings
    else:
        disc_settings = M.DiscoverySettings()
    return html.Div(
        [
            html.Div(
                [
                    html.P(
                        "Discovery settings explenation...",
                        className="mb-3 h6",
                    ),
                    dbc.Button(
                        "Learn more",
                        class_name="border-0 mb-3",
                        size="sm",
                        color="primary",
                        outline=True,
                        href=CREATE_PROJECT_DOCS_LINK,
                        target="_blank",
                        external_link=True,
                    ),
                ],
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
            html.Div(
                [
                    html.P(
                        "Explore settings explenation...",
                        className="mb-3 h6",
                    ),
                    dbc.Button(
                        "Learn more",
                        class_name="border-0 mb-3",
                        size="sm",
                        color="primary",
                        outline=True,
                        href=CREATE_PROJECT_DOCS_LINK,
                        target="_blank",
                        external_link=True,
                    ),
                ],
            ),
            create_form_property_input(
                property=PROP_EXPLORE_AUTO_REFRESH,
                index_type=PROJECT_INDEX_TYPE,
                icon_cls="bi bi-arrow-clockwise",
                value=webapp_settings.auto_refresh_enabled,
                component_type=dbc.Checkbox,
            ),
        ]
    )


@callback(
    Output(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    Input(CONFIRM_DIALOG_ACCEPT, "n_clicks"),
    State(MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
def delete_confirm_button_clicked(n_clicks: int, pathname: str) -> int:
    if n_clicks:
        project_id = P.get_path_value(
            P.PROJECTS_MANAGE_PATH, pathname, P.PROJECT_ID_PATH_PART
        )
        depenednecies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        try:
            project = depenednecies.storage.get_project(project_id=project_id)
            for edt in project.event_data_tables:
                depenednecies.storage.delete_event_data_table_definition(
                    project_id=project_id, edt_full_name=edt.get_full_name()
                )
            depenednecies.storage.delete_project(project_id)
        except Exception:
            # TBD: Toaster
            traceback.print_exc()

    return no_update


@callback(
    Output(CONFIRM_DIALOG_INDEX, "is_open"),
    Input(DELETE_BUTTON, "n_clicks"),
    Input(CONFIRM_DIALOG_CLOSE, "n_clicks"),
    prevent_initial_call=True,
)
def delete_button_clicked(delete: int, close: int) -> bool:
    if delete is None:
        return no_update
    return ctx.triggered_id == DELETE_BUTTON
