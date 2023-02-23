import dash_bootstrap_components as dbc
from dash import Input, Output, callback, html
import dash.development.base_component as bc
from typing import Optional
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted
from mitzu.webapp.helper import create_form_property_input
from mitzu.helper import value_to_label
from mitzu.webapp.pages.projects.helper import PROP_CONNECTION, PROJECT_INDEX_TYPE
import mitzu.webapp.pages.projects.event_tables_config as ETC
import dash_mantine_components as dmc
from uuid import uuid4

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


def create_project_settings(
    project: Optional[M.Project], dependencies: DEPS.Dependencies
) -> bc.Component:
    return html.Div(
        [
            dbc.Form(
                children=[
                    dbc.Accordion(
                        children=[
                            dbc.AccordionItem(
                                children=[
                                    html.P(
                                        [
                                            html.I(className="bi bi-gear me-1"),
                                            "Setup your project:",
                                        ],
                                        className="mb-3 lead",
                                    ),
                                    create_basic_project_settings(
                                        project, dependencies
                                    ),
                                ],
                                title="Project Settings",
                            ),
                            dbc.AccordionItem(
                                create_explore_settings(project),
                                title="Explore Settings",
                            ),
                            dbc.AccordionItem(
                                create_discovery_settings(project),
                                title="Discovery Settings",
                            ),
                            dbc.AccordionItem(
                                ETC.create_event_tables(project), title="Event Tables"
                            ),
                        ],
                    ),
                ],
                id=PROJECT_DETAILS_CONTAINER,
                class_name="mt-3 ",
            ),
        ],
    )


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
                value=project.description if project else None,
                index_type=PROJECT_INDEX_TYPE,
                component_type=dbc.Textarea,
                icon_cls="bi bi-blockquote-left",
                placeholder="Describe the project!",
                rows=4,
                maxlength=300,
            ),
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
                    html.I(className="bi bi-info-circle me-1"),
                    """Discovery settings are needed for the project discovery step. 
                    The discovery step scrapes the data warehouse tables for the columns and it's values.""",
                ],
                className="mb-3 lead",
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
                    html.I(className="bi bi-info-circle me-1"),
                    "Explore settings change the default behaviour of the explore page.",
                ],
                className="mb-3 lead",
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
