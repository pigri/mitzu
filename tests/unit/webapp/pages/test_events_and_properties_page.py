from flask import Flask
import mitzu.webapp.pages.manage_event_defs as MED
from mitzu.webapp.storage import SAMPLE_PROJECT_ID
from mitzu.webapp.dependencies import Dependencies
from unittest.mock import patch
from tests.helper import to_dict
import mitzu.webapp.helper as H
from typing import cast
from unittest.mock import MagicMock


@patch("mitzu.webapp.pages.manage_event_defs.ctx")
def test_discover_events_page(ctx, server: Flask, dependencies: Dependencies):
    with server.test_request_context():

        rows, message = MED.handle_project_discovery(
            set_progress=lambda *args: print(args[0][1]),
            discovery_clicks=1,
            project_id=SAMPLE_PROJECT_ID,
        )

        assert message == ""
        assert len(rows) == 8

        row = to_dict(rows[0])["children"]

        assert row[0]["props"]["children"] == "main.page_events"
        assert row[1]["props"]["children"] == "element_clicked_on_page"
        assert row[2]["props"]["children"] == "9 properties"

        row = to_dict(rows[1])["children"]

        assert row[0]["props"]["children"] == "main.page_events"
        assert row[1]["props"]["children"] == "page_visit"
        assert row[2]["props"]["children"] == "9 properties"

        row = to_dict(rows[2])["children"]

        assert row[0]["props"]["children"] == "main.search_events"
        assert row[1]["props"]["children"] == "search"
        assert row[2]["props"]["children"] == "6 properties"

        # Testing if the storage works properly

        sample_project = dependencies.storage.get_project(SAMPLE_PROJECT_ID)
        dp = sample_project._discovered_project.get_value()
        assert dp is not None

        evt_field_def = H.find_event_field_def("page_visit.acquisition_campaign", dp)

        assert evt_field_def._event_data_table.project is not None
        cast(
            MagicMock, dependencies.tracking_service.track_project_discovered
        ).assert_called_once()
