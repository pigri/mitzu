from typing import cast, List, Dict, Any

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import flask
from dash import html, register_page, callback, Output, Input, ALL, ctx, no_update, dcc

import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.paths as P
import mitzu.webapp.navbar as NB
from mitzu.webapp.auth.decorator import restricted_layout, restricted
from mitzu.webapp.helper import TBL_CLS, TBL_HEADER_CLS
import mitzu.webapp.cache as C
import pickle
import json
import traceback
import base64


DELETE_BUTTON_TYPE = "admin_delete_button_type"
STORAGE_TABLE = "admin_storage_table"
TBL_BODY_CONTAINER = "admin_table_body_container"
STORAGE_DOWNLOAD = "admin_storage_download"
STORAGE_RESET = "admin_reset"
ADMIN_NAVBAR = "admin_navbar"

STORAGE_DOWNLOAD_BUTTON = "admin_storage_download_button"
STORAGE_RESET_BUTTON = "admin_storage_reset_button"
CLEAR_LOCAL_CACHE_BUTTON = "clear_local_cache_button"
STORAGE_INFO = "admin_storage_info"


def create_table_body_rows(mitzu_cache: C.MitzuCache) -> List[html.Td]:
    all_keys = mitzu_cache.list_keys()
    all_keys = sorted(all_keys)
    rows = []
    for key in all_keys:
        rows.append(
            html.Tr(
                [
                    html.Td(key),
                    html.Td(
                        [
                            dbc.Button(
                                "Delete",
                                size="sm",
                                color="danger",
                                outline=True,
                                id={"type": DELETE_BUTTON_TYPE, "index": key},
                            ),
                        ],
                    ),
                ],
                className=TBL_CLS,
            )
        )
    return rows


@restricted_layout
def layout() -> bc.Component:
    cache = cast(DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)).cache

    table_header = html.Thead(
        html.Tr([html.Th("key"), html.Th("value")], className=TBL_HEADER_CLS)
    )
    rows = create_table_body_rows(cache)

    table = dbc.Table(
        [table_header, html.Tbody(rows, id=TBL_BODY_CONTAINER)],
        id=STORAGE_TABLE,
        bordered=True,
        hover=True,
        responsive=True,
        striped=True,
        size="sm",
    )
    return html.Div(
        [
            NB.create_mitzu_navbar(id=ADMIN_NAVBAR, create_explore_button=False),
            dcc.Download(id=STORAGE_DOWNLOAD),
            dbc.Container(
                children=[
                    html.H4("Mitzu Storage Content:", className="card-title mb-3"),
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Button(
                                    [
                                        html.B(className="bi bi-cloud-download me-1"),
                                        "Download json",
                                    ],
                                    id=STORAGE_DOWNLOAD_BUTTON,
                                    color="primary",
                                    size="sm",
                                ),
                                lg="auto",
                                sm=12,
                                class_name="mb-1",
                            ),
                            dbc.Col(
                                dcc.Upload(
                                    dbc.Button(
                                        [
                                            html.B(className="bi bi-cloud-upload me-1"),
                                            "Reset from json",
                                        ],
                                        id=STORAGE_RESET_BUTTON,
                                        color="info",
                                        size="sm",
                                    ),
                                    accept="application/json",
                                    multiple=False,
                                    id=STORAGE_RESET,
                                ),
                                lg="auto",
                                sm=12,
                                class_name="mb-1",
                            ),
                            dbc.Col(
                                children=[],
                                id=STORAGE_INFO,
                            ),
                        ]
                    ),
                    html.Hr(),
                    table,
                ]
            ),
        ]
    )


def validate_input_json(storage: Dict[str, str]) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    for k, v in storage.items():
        unpickled = pickle.loads(base64.b64decode(v.encode("ascii")))
        if unpickled is None:
            raise ValueError(f"Invalid values for key: {k}")
        res[k] = unpickled
    return res


@callback(
    Output(TBL_BODY_CONTAINER, "children"),
    Output(STORAGE_INFO, "children"),
    Input({"type": DELETE_BUTTON_TYPE, "index": ALL}, "n_clicks"),
    Input(STORAGE_RESET, "contents"),
    prevent_initial_call=True,
    background=True,
    running=[
        [Output(STORAGE_DOWNLOAD_BUTTON, "disabled"), True, False],
        [Output(STORAGE_RESET_BUTTON, "disabled"), True, False],
    ],
    interval=100,
)
@restricted
def update_tbl_body(delete_n_clicks: int, content: str):
    try:
        if ctx.triggered_id == STORAGE_RESET and content is not None:
            cache = cast(
                DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
            ).cache
            _, content_string = content.split(",")
            decoded = base64.b64decode(content_string)
            storage = json.loads(decoded.decode())
            to_restore = validate_input_json(storage)
            cache.clear_all()
            for k, v in to_restore.items():
                cache.put(k, v)
            res = create_table_body_rows(cache)
            return res, ""
        elif ctx.triggered_id != STORAGE_RESET and delete_n_clicks is not None:
            cache = cast(
                DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
            ).cache
            key = ctx.triggered_id["index"]
            cache.clear(key)
            return create_table_body_rows(cache), ""
        else:
            return no_update, no_update
    except Exception as exc:
        traceback.print_exc()
        return no_update, html.P(f"{str(exc)}", className="text-danger lead")


@callback(
    Output(STORAGE_DOWNLOAD, "data"),
    Input(STORAGE_DOWNLOAD_BUTTON, "n_clicks"),
    prevent_initial_call=True,
    background=True,
    running=[
        [Output(STORAGE_DOWNLOAD_BUTTON, "disabled"), True, False],
        [Output(STORAGE_RESET_BUTTON, "disabled"), True, False],
    ],
)
@restricted
def download_button_clicked(n_clicks: int):
    if n_clicks is not None:
        cache = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        ).cache
        all_keys = cache.list_keys(prefix="__", strip_prefix=False)
        all_keys = sorted(all_keys)
        res: Dict[str, Any] = {}

        for k in all_keys:
            value = cache.get(k)
            res[k] = base64.b64encode(pickle.dumps(value)).decode("ascii")
        res_str = json.dumps(res)
        return dict(content=res_str, filename="mitzu_storage.json")
    else:
        return no_update


register_page(
    __name__,
    path=P.ADMIN_PAGE,
    title="Mitzu - Admin",
)
