from flask import Flask
from tests.helper import to_dict, find_component_by_id
import mitzu.webapp.pages.admin_page as ADM
from unittest.mock import patch
import base64
from mitzu.webapp.dependencies import Dependencies
from mitzu.webapp.storage import SAMPLE_PROJECT_ID


def test_admin_page(server: Flask):
    with server.test_request_context():

        connections_comp = ADM.layout()
        page = to_dict(connections_comp)

        container = find_component_by_id(comp_id=ADM.TBL_BODY_CONTAINER, input=page)

        assert container is not None
        assert len(container["children"]) > 5


@patch("mitzu.webapp.pages.admin_page.ctx")
def test_download_and_reset_flow(ctx, server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        keys = dependencies.cache.list_keys()
        p = dependencies.storage.get_project(SAMPLE_PROJECT_ID)
        # Making sure the adapter is in State - we need to remove it before pickling
        p.get_adapter()

        # Downloading Storage
        res = ADM.download_button_clicked(1)
        base64_bytes = base64.b64encode(res["content"].encode("ascii")).decode()

        # Deleting 2 keys
        ctx.triggered_id = {"index": keys[0], "type": ADM.DELETE_BUTTON_TYPE}
        ADM.update_tbl_body(1, None)

        ctx.triggered_id = {"index": keys[1], "type": ADM.DELETE_BUTTON_TYPE}
        tbl_body, error_message = ADM.update_tbl_body(1, None)

        assert len(tbl_body) == len(keys) - 2
        assert error_message == ""

        # Reseting Storage from downloaded
        ctx.triggered_id = ADM.STORAGE_RESET
        tbl_body, error_message = ADM.update_tbl_body(None, f",{base64_bytes}")

        assert len(tbl_body) == len(keys)
        assert error_message == ""
