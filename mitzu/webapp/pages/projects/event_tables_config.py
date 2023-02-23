import dash_bootstrap_components as dbc
from dash import ALL, Input, Output, State, callback, ctx, html, no_update
import dash.development.base_component as bc
from typing import Callable, Dict, List, Optional, Tuple, cast, Set
import mitzu.model as M
from mitzu.webapp.pages.projects.helper import (
    PROP_CONNECTION,
    PROJECT_INDEX_TYPE,
    EDT_TBL_BODY,
    MISSING_FIELD,
    create_empty_edt,
    get_value_from_row,
)
from mitzu.webapp.auth.decorator import restricted
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.helper as H
import flask
import traceback
import dash_mantine_components as dmc


TBL_ID = "add_tables_id"
TBL_SEARCH_INPUT = "table_search_box"
TBL_PROGRESS_INFO = "table_progress_info"

TBL_CHECK_BOX_TYPE = "table_checkbox"
TBL_HEADER_CHECK_BOX_TYPE = "table_header_checkbox"

ADD_TABLES_BUTTON = "add_tables_button"
REMOVE_TABLES_BUTTON = "remove_tables_button"
CONFIGURE_TABLES_BUTTON = "configure_tables_button"

ADD_TABLES_MODAL = "add_tables_modal"
ADD_TABLES_MODAL_CLOSE = "add_tables_modal_close"
ADD_TABLES_MODAL_CONFIRM = "add_tables_modal_confirm"

CONF_TABLES_MODAL = "conf_tables_modal"
CONF_TABLES_MODAL_CLOSE = "conf_tables_modal_close"
CONF_TABLES_MODAL_CONFIRM = "conf_tables_modal_confirm"

CHOOSE_SCHEMA_INFO = "choose_schema_info"
CHOOSE_SCHEMA_DD = "choose_schema_dd"
CHOOSE_TABLES_CHECKLIST = "choose_tables_checklist"
CHOOSE_TABLES_INFO = "choose_tables_info"
SELECT_ALL_TABLES_BUTTON = "select_all_tables"

CONF_PROP_PROGRES_INFO = "conf_progree_info"
CONF_PROP_EVENT_TIME = "event_time_column"
CONF_PROP_USER_ID = "user_id_column"
CONF_PROP_EVENT_NAME_COLUMN = "event_name_column"
CONF_PROP_DATE_PARTITION = "date_partition_column"
CONF_PROP_IGNORE_COLUMN = "ignore_columns"

EDT_INDEX_TYPE = "edt_property_type"
EDT_VALIDATE_BUTTON = "edt_validate_button"
UPDATE_INTERVAL = 100


def create_table_row(edt: M.EventDataTable) -> html.Tr:
    return html.Tr(
        children=[
            html.Td(
                dbc.Checkbox(
                    id={
                        "type": TBL_CHECK_BOX_TYPE,
                        "index": edt.table_name,
                    },
                    value=False,
                ),
                className=H.TBL_CLS,
            ),
            html.Td(children=edt.get_full_name(), className=H.TBL_CLS),
            html.Td(
                children=edt.user_id_field._get_name(),
                className=H.TBL_CLS
                if edt.user_id_field != MISSING_FIELD
                else H.TBL_CLS_WARNING,
            ),
            html.Td(
                children=edt.event_time_field._get_name(),
                className=H.TBL_CLS
                if edt.event_time_field != MISSING_FIELD
                else H.TBL_CLS_WARNING,
            ),
            html.Td(
                children=(
                    edt.event_name_field._get_name() if edt.event_name_field else None
                ),
                className=H.TBL_CLS,
            ),
            html.Td(
                children=edt.date_partition_field._get_name()
                if edt.date_partition_field
                else None,
                className=H.TBL_CLS,
            ),
            html.Td(",".join(edt.ignored_fields), className=H.TBL_CLS),
        ]
    )


def create_table_component(project: Optional[M.Project]):
    table_header = [
        html.Thead(
            html.Tr(
                children=[
                    html.Th(
                        dbc.Checkbox(id=TBL_HEADER_CHECK_BOX_TYPE),
                        className=H.TBL_HEADER_CLS,
                    ),
                    html.Th("Table*", className=H.TBL_HEADER_CLS),
                    html.Th("User id column*", className=H.TBL_HEADER_CLS),
                    html.Th("Event time column*", className=H.TBL_HEADER_CLS),
                    html.Th("Event name column", className=H.TBL_HEADER_CLS),
                    html.Th("Date partition column", className=H.TBL_HEADER_CLS),
                    html.Th("Ignore columns", className=H.TBL_HEADER_CLS),
                ],
            )
        )
    ]
    rows = []
    if project is not None:
        for edt in project.event_data_tables:
            rows.append(create_table_row(edt))

    table_body = [html.Tbody(children=rows, id=EDT_TBL_BODY)]

    return dbc.Table(
        children=table_header + table_body,
        hover=False,
        responsive=True,
        striped=True,
        size="sm",
        id=TBL_ID,
    )


def create_add_tables_modal() -> dbc.Modal:
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Add tables"), close_button=False),
            dbc.ModalBody(
                [
                    html.Div(
                        [
                            dbc.Label(
                                "Select schema",
                                class_name="lead w-50",
                                id=CHOOSE_SCHEMA_INFO,
                            ),
                            dmc.Select(
                                id=CHOOSE_SCHEMA_DD,
                                className="w-50 d-inline-block",
                                placeholder="Loading...",
                                data=[],
                                size="xs",
                                searchable=True,
                            ),
                        ]
                    ),
                    html.Hr(),
                    dbc.Label(
                        children=[],
                        id=CHOOSE_TABLES_INFO,
                        class_name="mb-3 d-block lead",
                    ),
                    dbc.Checklist(
                        id=CHOOSE_TABLES_CHECKLIST,
                        options=[],
                        value=[],
                        class_name="ms-3 mb-3",
                        style={"min-height": "100px"},
                        label_class_name="small",
                    ),
                    dbc.Button(
                        children=[
                            html.B(className="bi bi-check2-all me-1"),
                            "Select all",
                        ],
                        id=SELECT_ALL_TABLES_BUTTON,
                        size="sm",
                        color="secondary",
                        class_name="mb-3 d-inline-block me-1",
                        disabled=True,
                    ),
                ],
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        [html.B(className="bi bi-x me-1"), "Close"],
                        id=ADD_TABLES_MODAL_CLOSE,
                        size="sm",
                        color="secondary",
                        class_name="me-1",
                    ),
                    dbc.Button(
                        [html.B(className="bi bi-plus-circle me-1"), "Add tables"],
                        id=ADD_TABLES_MODAL_CONFIRM,
                        size="sm",
                        color="success",
                    ),
                ]
            ),
        ],
        id=ADD_TABLES_MODAL,
        scrollable=True,
        is_open=False,
        backdrop="static",
    )


def create_configure_tables_modal() -> dbc.Modal:
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Configure tables"), close_button=False),
            dbc.ModalBody(
                dbc.Form(
                    [
                        html.Div(
                            [
                                html.Div(
                                    "Loading table columns",
                                    className="lead d-block mb-3 d-inline-block",
                                    id=CONF_PROP_PROGRES_INFO,
                                ),
                            ]
                        ),
                        H.create_form_property_input(
                            property=CONF_PROP_USER_ID,
                            index_type=EDT_INDEX_TYPE,
                            component_type=dmc.MultiSelect,
                            data=[],
                            value=None,
                            required=True,
                            icon_cls="bi bi-person-circle",
                            label_lg=4,
                            label_sm=4,
                            input_lg=8,
                            input_sm=8,
                            size="xs",
                            searchable=True,
                            placeholder="Loading...",
                            dropdownPosition="bottom",
                        ),
                        H.create_form_property_input(
                            property=CONF_PROP_EVENT_TIME,
                            index_type=EDT_INDEX_TYPE,
                            component_type=dmc.MultiSelect,
                            data=[],
                            value=None,
                            required=True,
                            icon_cls="bi bi-clock",
                            label_lg=4,
                            label_sm=4,
                            input_lg=8,
                            input_sm=8,
                            size="xs",
                            searchable=True,
                            placeholder="Loading...",
                            dropdownPosition="bottom",
                        ),
                        H.create_form_property_input(
                            property=CONF_PROP_EVENT_NAME_COLUMN,
                            index_type=EDT_INDEX_TYPE,
                            component_type=dmc.MultiSelect,
                            data=[],
                            value=None,
                            icon_cls="bi bi-play-btn",
                            label_lg=4,
                            label_sm=4,
                            input_lg=8,
                            input_sm=8,
                            size="xs",
                            searchable=True,
                            clearable=True,
                            placeholder="Loading...",
                            dropdownPosition="bottom",
                        ),
                        H.create_form_property_input(
                            property=CONF_PROP_DATE_PARTITION,
                            index_type=EDT_INDEX_TYPE,
                            component_type=dmc.MultiSelect,
                            data=[],
                            value=None,
                            icon_cls="bi bi-calendar2-check",
                            label_lg=4,
                            label_sm=4,
                            input_lg=8,
                            input_sm=8,
                            size="xs",
                            searchable=True,
                            clearable=True,
                            placeholder="Loading...",
                            dropdownPosition="bottom",
                        ),
                        H.create_form_property_input(
                            property=CONF_PROP_IGNORE_COLUMN,
                            index_type=EDT_INDEX_TYPE,
                            component_type=dmc.MultiSelect,
                            data=[],
                            value=None,
                            icon_cls="bi bi-file-x",
                            label_lg=4,
                            label_sm=4,
                            input_lg=8,
                            input_sm=8,
                            size="xs",
                            clearable=True,
                            searchable=True,
                            placeholder="Loading...",
                            dropdownPosition="bottom",
                        ),
                        html.Div("", style={"min-height": "200px"}),
                    ],
                ),
                class_name="my-3",
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        [html.B(className="bi bi-x me-1"), "Close"],
                        id=CONF_TABLES_MODAL_CLOSE,
                        size="sm",
                        color="secondary",
                        class_name="me-1",
                    ),
                    dbc.Button(
                        [html.B(className="bi bi-check-circle me-1"), "Configure"],
                        id=CONF_TABLES_MODAL_CONFIRM,
                        size="sm",
                        color="success",
                    ),
                ]
            ),
        ],
        id=CONF_TABLES_MODAL,
        is_open=False,
        scrollable=True,
        backdrop="static",
    )


def create_event_tables(project: Optional[M.Project]) -> bc.Component:
    table = create_table_component(project)
    add_tables_modal = create_add_tables_modal()
    configure_tables_modal = create_configure_tables_modal()
    return html.Div(
        [
            html.P(
                children=[
                    html.I(className="bi bi-info-circle me-1"),
                    """Event tables are tables in the data warehouse that contain user events. 
                The user id and event time columns are mandatory for the event tables. 
                The event name column is optional and reserved for 
                data warehouse tables that contain multiple event types.""",
                ],
                className="mb-3 lead",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Button(
                            [html.B(className="bi bi-plus-circle me-1"), "Add tables"],
                            color="primary",
                            id=ADD_TABLES_BUTTON,
                        ),
                        width="auto",
                        class_name="mb-3",
                    ),
                    dbc.Col(
                        dbc.Button(
                            [html.B(className="bi bi-gear me-1"), "Configure"],
                            color="secondary",
                            disabled=True,
                            id=CONFIGURE_TABLES_BUTTON,
                        ),
                        class_name="mb-3",
                        width="auto",
                    ),
                    dbc.Col(
                        dbc.Button(
                            [html.B(className="bi bi-x-circle me-1"), "Remove"],
                            color="danger",
                            disabled=True,
                            id=REMOVE_TABLES_BUTTON,
                        ),
                        class_name="mb-3",
                        width="auto",
                    ),
                    dbc.Col(
                        dbc.Input(id=TBL_SEARCH_INPUT, placeholder="Search tables"),
                        class_name="ms-auto mb-3",
                        width="3",
                    ),
                ],
                class_name="me-auto",
            ),
            table,
            html.Hr(),
            dbc.Button(
                [
                    html.B(className="bi bi-check-circle me-1"),
                    "Validate",
                ],
                id=EDT_VALIDATE_BUTTON,
                color="primary",
                className="me-3 d-inline-block shadow-sm mb-3",
            ),
            html.Div(
                children=[],
                id=TBL_PROGRESS_INFO,
                className="lead d-inline-block mb-3",
            ),
            add_tables_modal,
            configure_tables_modal,
        ],
        className="overflow-auto mh-100 mt-3",
    )


def get_checkbox_value_from_row(tr: html.Tr) -> bool:
    return (
        tr.get("props")
        .get("children")[0]
        .get("props")
        .get("children")
        .get("props")["value"]
    )


def _get_unioned_table_fields(
    set_progress,
    adapter,
    all_selected_tables_count: int,
    tbl_body_children: List[bc.Component],
) -> Tuple[Dict[str, int], List[Set]]:
    """If the user selects tables with the checkboxes to configure,
    this method returns all possible fields unioned for those tables.
    Also it returns the values that were already chosen based on the table cells (user_id, event_time, etc.).
    However it cross checks them with the state of the fields from the DWH.
    """
    count = 0
    unioned_table_fields: Dict[
        str, int
    ] = {}  # for counting occurances of field in all tables
    collected_prop_values: List[Set[str]] = [set(), set(), set(), set(), set()]
    for tr in tbl_body_children:
        check_box = get_checkbox_value_from_row(tr)
        if check_box:
            table_name_children = get_value_from_row(tr, 1)
            fields_from_table = [get_value_from_row(tr, i) for i in range(2, 7)]
            if type(table_name_children) == str:
                table_parts = table_name_children.split(".")
                schema = table_parts[0]
                table_name = table_parts[-1]
                fields = adapter.list_all_table_columns(schema, table_name)
                for field in fields:
                    for f in field.get_all_subfields():
                        tbl_field_name = f._get_name()
                        val = unioned_table_fields.get(tbl_field_name)
                        unioned_table_fields[tbl_field_name] = (
                            1 if val is None else val + 1
                        )
                        for i, field in enumerate(fields_from_table):
                            if field:
                                if i < 5 and tbl_field_name == field:
                                    collected_prop_values[i].add(tbl_field_name)
                                elif tbl_field_name in field.split(","):
                                    # Ignore fields is a list of fields separated by comma
                                    collected_prop_values[i].add(tbl_field_name)

            count += 1
            set_progress(
                f"Loading table columns {count*100/all_selected_tables_count:.0f}%"
            )
    return unioned_table_fields, collected_prop_values


def find_first_field(
    all_fields: Dict[str, M.Field], field_names: List[str]
) -> Optional[M.Field]:
    for fn in field_names:
        field = all_fields.get(fn)
        if field:
            return field
    return None


@callback(
    Output(CONFIGURE_TABLES_BUTTON, "disabled"),
    Output(REMOVE_TABLES_BUTTON, "disabled"),
    Output({"type": TBL_CHECK_BOX_TYPE, "index": ALL}, "value"),
    Input(TBL_HEADER_CHECK_BOX_TYPE, "value"),
    Input({"type": TBL_CHECK_BOX_TYPE, "index": ALL}, "value"),
    State(EDT_TBL_BODY, "children"),
    prevent_initial_call=True,
)
@restricted
def manage_table_checkboxes(
    header_checkbox: bool, tr_checkboxes: List, tbl_rows: List
) -> Tuple[bool, bool, List[bool]]:
    if ctx.triggered_id == TBL_HEADER_CHECK_BOX_TYPE:
        checkboxes: List[bool] = []
        for tr in tbl_rows:
            style = tr["props"].get("style", {}).get("display", "table-row")
            if style == "table-row":
                checkboxes.append(header_checkbox)
            else:
                checkboxes.append(get_checkbox_value_from_row(tr))
        disabled = not any(checkboxes)
        return (
            disabled,
            disabled,
            checkboxes,
        )
    else:
        tr_checkboxes = [get_checkbox_value_from_row(tr) for tr in tbl_rows]
        disabled = not any(tr_checkboxes)
        return (disabled, disabled, tr_checkboxes)


@callback(
    Output(ADD_TABLES_MODAL, "is_open"),
    Input(ADD_TABLES_BUTTON, "n_clicks"),
    Input(ADD_TABLES_MODAL_CLOSE, "n_clicks"),
    Input(ADD_TABLES_MODAL_CONFIRM, "n_clicks"),
)
@restricted
def manage_add_table_modal_open(add_tables: int, close: int, confirm: int) -> bool:
    return ctx.triggered_id == ADD_TABLES_BUTTON


@callback(
    Output(CONF_TABLES_MODAL, "is_open"),
    Input(CONF_TABLES_MODAL_CLOSE, "n_clicks"),
    Input(CONF_TABLES_MODAL_CONFIRM, "n_clicks"),
    Input(CONFIGURE_TABLES_BUTTON, "n_clicks"),
)
@restricted
def manage_configure_table_modal_open(close: int, confirm: int, configure: int) -> bool:
    return ctx.triggered_id == CONFIGURE_TABLES_BUTTON


@callback(
    Output(CHOOSE_SCHEMA_DD, "data"),
    Output(CHOOSE_SCHEMA_DD, "value"),
    Output(CHOOSE_SCHEMA_DD, "placeholder"),
    Input(ADD_TABLES_BUTTON, "n_clicks"),
    Input(ADD_TABLES_MODAL_CLOSE, "n_clicks"),
    Input(ADD_TABLES_MODAL_CONFIRM, "n_clicks"),
    State({"type": PROJECT_INDEX_TYPE, "index": PROP_CONNECTION}, "value"),
    background=True,
    running=[
        (
            Output(CHOOSE_SCHEMA_DD, "placeholder"),
            "Loading schemas...",
            "Select schema",
        ),
        (
            Output(CHOOSE_SCHEMA_INFO, "children"),
            [
                dbc.Spinner(
                    spinner_style={"width": "1rem", "height": "1rem"},
                    spinner_class_name="me-1",
                ),
                "Loading schemas...",
            ],
            "Select schema:",
        ),
    ],
    interval=UPDATE_INTERVAL,
    prevent_initial_call=True,
)
@restricted
def manage_choose_schema_dropdown(
    add_tables: int, close: int, confirm: int, connection_id: Optional[str]
) -> Tuple:
    if ctx.triggered_id in [ADD_TABLES_MODAL_CLOSE, ADD_TABLES_MODAL_CONFIRM]:
        return ([], None, "Select schema")
    if connection_id is not None:
        try:
            dependencies: DEPS.Dependencies = cast(
                DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
            )
            connection = dependencies.storage.get_connection(connection_id)
            dummy_project = M.Project(
                connection=connection,
                event_data_tables=[],
                project_name="dummy_project",
            )
            adapter = dummy_project.get_adapter()
            schemas = [{"label": s, "value": s} for s in adapter.list_schemas()]
            return (schemas, no_update, "Select schema")
        except Exception:
            traceback.print_exc()
    return ([], None, "Something went wrong...")


@callback(
    Output(CHOOSE_TABLES_CHECKLIST, "options"),
    Output(CHOOSE_TABLES_CHECKLIST, "value"),
    Output(CHOOSE_TABLES_INFO, "children"),
    Input(CHOOSE_SCHEMA_DD, "value"),
    Input(SELECT_ALL_TABLES_BUTTON, "n_clicks"),
    Input(ADD_TABLES_MODAL_CLOSE, "n_clicks"),
    Input(ADD_TABLES_MODAL_CONFIRM, "n_clicks"),
    State({"type": PROJECT_INDEX_TYPE, "index": PROP_CONNECTION}, "value"),
    State(CHOOSE_TABLES_CHECKLIST, "options"),
    State(CHOOSE_TABLES_CHECKLIST, "value"),
    background=True,
    running=[
        (
            Output(CHOOSE_TABLES_INFO, "children"),
            [
                dbc.Spinner(
                    spinner_style={"width": "1rem", "height": "1rem"},
                    spinner_class_name="me-1",
                ),
                "Loading tables in schema",
            ],
            "Choose tables to add",
        ),
        (
            Output(SELECT_ALL_TABLES_BUTTON, "disabled"),
            True,
            False,
        ),
    ],
    interval=UPDATE_INTERVAL,
    cancel=[Input(ADD_TABLES_MODAL_CLOSE, "n_clicks")],
    prevent_initial_call=True,
)
@restricted
def manage_choose_tables_checklist(
    schema: Optional[str],
    select_all_clicks: int,
    close: int,
    confirm: int,
    connection_id: Optional[str],
    options: List,
    selected_values: List,
) -> Tuple:
    if ctx.triggered_id in [ADD_TABLES_MODAL_CLOSE, ADD_TABLES_MODAL_CONFIRM]:
        return ([], [], "No schema chosen")

    if schema is None:
        return ([], [], "No schema chosen")

    if ctx.triggered_id == SELECT_ALL_TABLES_BUTTON:
        if len(selected_values) == len(options):
            return (options, [], "Choose tables to add")
        else:
            vals = [o.get("value") for o in options]
            return (options, vals, "Choose tables to add")

    if connection_id is not None:
        try:
            dependencies: DEPS.Dependencies = cast(
                DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
            )
            connection = dependencies.storage.get_connection(connection_id)
            dummy_project = M.Project(
                connection=connection,
                event_data_tables=[],
                project_name="dummy_project",
            )
            adapter = dummy_project.get_adapter()

            tables = [
                {"label": s, "value": s} for s in adapter.list_tables(schema=schema)
            ]
            return (tables, [], "Choose tables to add")
        except Exception as exc:
            traceback.print_exc()
            return ([], [], f"Something went wrong: {exc}")
    return ([], [], "Connection must be set first!")


@callback(
    Output({"type": EDT_INDEX_TYPE, "index": ALL}, "data"),
    Output({"type": EDT_INDEX_TYPE, "index": ALL}, "value"),
    Output({"type": EDT_INDEX_TYPE, "index": ALL}, "placeholder"),
    Output(CONF_PROP_PROGRES_INFO, "children"),
    Input(CONFIGURE_TABLES_BUTTON, "n_clicks"),
    Input(CONF_TABLES_MODAL_CLOSE, "n_clicks"),
    Input(CONF_TABLES_MODAL_CONFIRM, "n_clicks"),
    State({"type": PROJECT_INDEX_TYPE, "index": PROP_CONNECTION}, "value"),
    State(EDT_TBL_BODY, "children"),
    background=True,
    interval=UPDATE_INTERVAL,
    progress=Output(CONF_PROP_PROGRES_INFO, "children"),
    cancel=[Input(CONF_TABLES_MODAL_CLOSE, "n_clicks")],
    prevent_initial_call=True,
)
@restricted
def manage_configure_property_inputs(
    set_progress,
    configure: int,
    close: int,
    confirm: int,
    connection_id: Optional[str],
    tbl_body_children: List,
) -> Tuple:
    if ctx.triggered_id in [CONF_TABLES_MODAL_CLOSE, CONF_TABLES_MODAL_CONFIRM]:
        # Setting the default values as we closed the dialog
        return (
            [[] for _ in range(0, 5)],
            [None for _ in range(0, 5)],
            ["Loading..." for _ in range(0, 5)],
            "Loading table column",
        )

    if connection_id is not None:
        try:
            dependencies: DEPS.Dependencies = cast(
                DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
            )
            connection = dependencies.storage.get_connection(connection_id)
            dummy_project = M.Project(
                connection=connection,
                event_data_tables=[],
                project_name="dummy_project",
            )
            all_selected_count = 0
            for tr in tbl_body_children:
                if get_checkbox_value_from_row(tr):
                    all_selected_count += 1
            all_table_fields, collected_values_list = _get_unioned_table_fields(
                set_progress,
                adapter=dummy_project.get_adapter(),
                all_selected_tables_count=all_selected_count,
                tbl_body_children=tbl_body_children,
            )
            unioned_table_fields = list(all_table_fields.items())
            unioned_table_fields = sorted(
                unioned_table_fields,
                key=lambda v: f"{str(999-v[1]*100).zfill(3)}-{v[0]}",
            )  # Sort order based on priority
            options = [
                {
                    "label": f"{k} {f'(missing from {all_selected_count-v})' if v != all_selected_count else ''}",
                    "value": k,
                }
                for k, v in unioned_table_fields
            ]

            info_text = f"Select properties for {all_selected_count} table{'s' if all_selected_count>1 else ''}:"

            return (
                [options for _ in range(0, 5)],
                [list(vals) for vals in collected_values_list],
                ["Select column", "Select column", "Optional", "Optional", "Optional"],
                info_text,
            )
        except Exception:
            traceback.print_exc()
    return (
        [[] for _ in range(0, 5)],
        [None for _ in range(0, 5)],
        [None for _ in range(0, 5)],
        "Something went wrong. Make sure your selected tables are validated.",
    )


def handle_configure_modal_confirm(
    set_progress,
    connection_id: str,
    tbl_body_children: List[bc.Component],
    edt_properties: List[bc.Component],
) -> List[bc.Component]:
    dependencies: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    connection = dependencies.storage.get_connection(connection_id)
    dummy_project = M.Project(
        connection=connection,
        event_data_tables=[],
        project_name="dummy_project",
    )
    adapter = dummy_project.get_adapter()
    results_tbl_children = []
    count = 0
    sel_count = 0

    for tr in tbl_body_children:
        if get_checkbox_value_from_row(tr):
            sel_count += 1

    for tr in tbl_body_children:
        check_box = get_checkbox_value_from_row(tr)
        if check_box:
            count += 1
            set_progress(f"Validating {count}/{sel_count} tables")
            full_table_name = get_value_from_row(tr, 1)
            schema, table_name = tuple(full_table_name.split("."))
            fields = adapter.list_all_table_columns(
                table_name=table_name, schema=schema
            )
            field_names: Dict[str, M.Field] = {}
            for field in fields:
                for f in field.get_all_subfields():
                    field_names[f._get_name()] = f

            user_id_fields = edt_properties[0]
            evnet_time_fields = edt_properties[1]
            event_name_fields = edt_properties[2]
            date_partition_fields = edt_properties[3]
            ignored_fields = edt_properties[4]
            ignored_fields = ignored_fields if ignored_fields is not None else []

            user_id_field = find_first_field(field_names, user_id_fields)
            event_time_field = find_first_field(field_names, evnet_time_fields)
            edt = M.EventDataTable(
                table_name=table_name,
                schema=schema,
                user_id_field=(user_id_field if user_id_field else MISSING_FIELD),
                event_time_field=(
                    event_time_field if event_time_field else MISSING_FIELD
                ),
                event_name_field=find_first_field(field_names, event_name_fields),
                date_partition_field=find_first_field(
                    field_names, date_partition_fields
                ),
                ignored_fields=[i for i in ignored_fields if i in field_names],
            )
            results_tbl_children.append(create_table_row(edt))
        else:
            results_tbl_children.append(tr)
    return results_tbl_children


def handle_validate_button_clicked(
    set_progress,
    connection_id: str,
    tbl_body_children: List[bc.Component],
) -> List[bc.Component]:
    dependencies: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    connection = dependencies.storage.get_connection(connection_id)
    dummy_project = M.Project(
        connection=connection,
        event_data_tables=[],
        project_name="dummy_project",
    )
    adapter = dummy_project.get_adapter()
    sel_count = len(tbl_body_children)
    for i, tr in enumerate(tbl_body_children):
        set_progress(f"Validating {i+1}/{sel_count} tables")
        tr_children = tr["props"]["children"]
        full_table_name = get_value_from_row(tr, 1)
        user_id_col = get_value_from_row(tr, 2)
        event_time_col = get_value_from_row(tr, 3)
        date_partition_col = get_value_from_row(tr, 4)
        ignore_fields_str = get_value_from_row(tr, 5)
        schema, table_name = tuple(full_table_name.split("."))
        ignore_fields = (
            ignore_fields_str.split(",") if ignore_fields_str is not None else []
        )

        try:
            fields = adapter.list_all_table_columns(schema, table_name)
            tr_children[1]["props"]["className"] = H.TBL_CLS

            col_names: List[str] = []
            for field in fields:
                col_names.extend([f._get_name() for f in field.get_all_subfields()])

            tr_children[2]["props"]["className"] = (
                H.TBL_CLS if user_id_col in col_names else H.TBL_CLS_WARNING
            )
            tr_children[3]["props"]["className"] = (
                H.TBL_CLS if event_time_col in col_names else H.TBL_CLS_WARNING
            )
            tr_children[4]["props"]["className"] = (
                H.TBL_CLS if date_partition_col in col_names else H.TBL_CLS_WARNING
            )
            tr_children[5]["props"]["className"] = (
                H.TBL_CLS
                if len([f for f in ignore_fields if f not in col_names]) == 0
                else H.TBL_CLS_WARNING
            )
        except Exception:
            traceback.print_exc()
            for i in range(1, 6):
                tr_children[i]["props"]["className"] = H.TBL_CLS_WARNING

    return tbl_body_children


@callback(
    Output(EDT_TBL_BODY, "children"),
    Input(ADD_TABLES_MODAL_CONFIRM, "n_clicks"),
    Input(CONF_TABLES_MODAL_CONFIRM, "n_clicks"),
    Input(REMOVE_TABLES_BUTTON, "n_clicks"),
    Input(TBL_SEARCH_INPUT, "value"),
    Input(EDT_VALIDATE_BUTTON, "n_clicks"),
    State(CHOOSE_SCHEMA_DD, "value"),
    State(CHOOSE_TABLES_CHECKLIST, "value"),
    State(EDT_TBL_BODY, "children"),
    State({"type": EDT_INDEX_TYPE, "index": ALL}, "value"),
    State({"type": PROJECT_INDEX_TYPE, "index": PROP_CONNECTION}, "value"),
    prevent_initial_call=True,
    background=True,
    running=[
        (
            Output(TBL_ID, "className"),
            "opacity-50",
            "",
        ),
    ],
    progress=Output(TBL_PROGRESS_INFO, "children"),
    interval=UPDATE_INTERVAL,
)
@restricted
def manage_event_data_table_body(
    set_progress: Callable,
    add_tables: int,
    configure_table: int,
    remove_tables: int,
    search_value: str,
    validate_nclicks: int,
    choose_schema_dd_value: str,
    tables: List,
    tbl_body_children: List,
    edt_properties: List,
    connection_id: str,
) -> List:
    set_progress("")
    if ctx.triggered_id == REMOVE_TABLES_BUTTON:
        return [
            row for row in tbl_body_children if not get_checkbox_value_from_row(row)
        ]

    if ctx.triggered_id == EDT_VALIDATE_BUTTON:
        return handle_validate_button_clicked(
            set_progress=set_progress,
            connection_id=connection_id,
            tbl_body_children=tbl_body_children,
        )

    if ctx.triggered_id == CONF_TABLES_MODAL_CONFIRM:
        return handle_configure_modal_confirm(
            set_progress=set_progress,
            connection_id=connection_id,
            tbl_body_children=tbl_body_children,
            edt_properties=edt_properties,
        )

    if ctx.triggered_id == TBL_SEARCH_INPUT:
        for tr in tbl_body_children:
            full_table_name = get_value_from_row(tr, 1)
            hidden = search_value not in full_table_name
            tr["props"]["style"] = {"display": "none" if hidden else "table-row"}

    if ctx.triggered_id == ADD_TABLES_MODAL_CONFIRM:
        already_present: List[str] = []
        for tr in tbl_body_children:
            already_present.append(get_value_from_row(tr, 1))

        for tbl in tables:
            if f"{choose_schema_dd_value}.{tbl}" in already_present:
                continue
            edt = create_empty_edt(choose_schema_dd_value, tbl)
            tbl_body_children.append(create_table_row(edt))
    return tbl_body_children
