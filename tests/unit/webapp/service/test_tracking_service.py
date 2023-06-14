from unittest.mock import MagicMock, patch
import mitzu.model as M
from datetime import datetime
from mitzu.webapp.service.tracking_service import AuthorizedTrackingService
import mitzu.webapp.configs as C

C.TRACKING_API_KEY = "api_key"
C.TRACKING_HOST = "host"


@patch("mitzu.webapp.service.tracking_service.analytics")
def test_track_event_called_for_discovered_project(
    analytics, discovered_project: M.DiscoveredProject
):

    authorizer = MagicMock()
    authorizer.get_current_user_id.return_value = "test_user"
    ts = AuthorizedTrackingService(authorizer)

    ts.track_project_discovered(discovered_project)
    analytics.track.assert_called_once_with(
        user_id="test_user",
        event="project_discovered",
        properties={
            "project_id": "sample_project_id",
            "project_name": "Sample ecommerce project",
            "connection_id": "sample_connection_id",
            "event_count": 8,
            "event_names": (
                "element_clicked_on_page,page_visit,search,"
                "add_to_cart,checkout,subscribe,email_sent,email_opened"
            ),
            "app_version": "0.0.0",
            "environment": "dev",
        },
    )


@patch("mitzu.webapp.service.tracking_service.analytics")
def test_track_event_called_for_explore(
    analytics, discovered_project: M.DiscoveredProject
):
    authorizer = MagicMock()
    authorizer.get_current_user_id.return_value = "test_user"

    ts = AuthorizedTrackingService(authorizer)
    m = discovered_project.create_notebook_class_model()
    metric = m.page_visit >> m.checkout

    ts.track_explore_finished(metric, 1, True)
    analytics.track.assert_called_once_with(
        user_id="test_user",
        event="metric_explore_finished",
        properties={
            "project_id": "sample_project_id",
            "project_name": "Sample ecommerce project",
            "connection_id": "sample_connection_id",
            "connection_type": "sqlite",
            "metric_type": "conversion",
            "segments": 2,
            "serialized": '{"segs": [{"l": {"en": "page_visit"}}, {"l": {"en": "checkout"}}]}',
            "duration_seconds": 1,
            "from_cache": True,
            "app_version": "0.0.0",
            "environment": "dev",
        },
    )


@patch("mitzu.webapp.service.tracking_service.analytics")
def test_track_event_called_for_connection(
    analytics, discovered_project: M.DiscoveredProject
):
    authorizer = MagicMock()
    authorizer.get_current_user_id.return_value = "test_user"

    ts = AuthorizedTrackingService(authorizer)

    ts.track_connection_saved(discovered_project.project.connection)
    analytics.track.assert_called_once_with(
        user_id="test_user",
        event="connection_saved",
        properties={
            "connection_id": "sample_connection_id",
            "connection_name": "Sample connection",
            "catalog": None,
            "host": "sample_project",
            "connection_type": "sqlite",
            "app_version": "0.0.0",
            "environment": "dev",
        },
    )


@patch("mitzu.webapp.service.tracking_service.analytics")
def test_track_event_called_for_project(
    analytics, discovered_project: M.DiscoveredProject
):
    authorizer = MagicMock()
    authorizer.get_current_user_id.return_value = "test_user"

    ts = AuthorizedTrackingService(authorizer)

    ts.track_project_saved(discovered_project.project)
    analytics.track.assert_called_once_with(
        user_id="test_user",
        event="project_saved",
        properties={
            "project_id": "sample_project_id",
            "project_name": "Sample ecommerce project",
            "connection_id": "sample_connection_id",
            "table_names": (
                "main.page_events,main.search_events,main.add_to_carts,"
                "main.checkouts,main.email_subscriptions,"
                "main.email_sent_events,main.email_opened_events"
            ),
            "discovery_sample_size": 2000,
            "explore_end_date_config": "custom_date",
            "explore_custom_end_date": datetime(2022, 1, 1, 0, 0),
            "app_version": "0.0.0",
            "environment": "dev",
        },
    )
