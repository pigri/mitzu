import mitzu.model as M
from dash import html

PROP_CONNECTION = "connection"
PROJECT_INDEX_TYPE = "project_property"
EDT_TBL_BODY = "edt_table_body"

MISSING_VALUE = "<Required>"
MISSING_FIELD = M.Field(MISSING_VALUE, M.DataType.STRING)


def create_empty_edt(schema: str, table_name: str):
    return M.EventDataTable(
        table_name=table_name,
        schema=schema,
        user_id_field=MISSING_FIELD,
        event_time_field=MISSING_FIELD,
    )


def get_value_from_row(tr: html.Tr, col: int) -> str:
    return tr.get("props").get("children")[col].get("props")["children"]
