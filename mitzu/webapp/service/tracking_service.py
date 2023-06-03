from __future__ import annotations

import mitzu.webapp.auth.authorizer as AU
import mitzu.webapp.configs as C
import segment.analytics as analytics
from typing import Any, Dict, Optional
import mitzu.helper as H
import mitzu.model as M
import flask
import mitzu.serialization as SE
import json
from mitzu import __version__
from abc import ABC, abstractmethod


def log_error_message(error, items):
    H.LOGGER.warn(f"Tracking error: {error}")


def init_analytics():
    H.LOGGER.debug(f"Tracking init: {C.TRACKING_HOST}")
    analytics.write_key = C.TRACKING_API_KEY
    analytics.host = C.TRACKING_HOST
    analytics.on_error = log_error_message
    analytics.sync_mode = True


class TrackingService(ABC):
    @abstractmethod
    def _get_current_user_id(self) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def register_new_user(self, email: str, role: str, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def track_page_view(self, request: flask.Request, response: flask.Response):
        raise NotImplementedError()

    @abstractmethod
    def track_connection_saved(self, connection: M.Connection):
        raise NotImplementedError()

    @abstractmethod
    def track_project_saved(self, project: M.Project):
        raise NotImplementedError()

    @abstractmethod
    def track_project_discovered(self, discovered_project: M.DiscoveredProject):
        raise NotImplementedError()

    @abstractmethod
    def track_explore_finished(
        self, metric: M.Metric, duration_seconds: float, from_cache: bool
    ):
        raise NotImplementedError()


class AuthorizedTrackingService(TrackingService):
    """
    Tracking service is an abstraction for usage tracking with Jitsu.
    Jitsu is compatible with Segment logging - so we are using the segment python api
    """

    def __init__(self, authorizer: AU.Authorizer):
        self._authorizer = authorizer
        init_analytics()

    def _get_current_user_id(self) -> Optional[str]:
        return self._authorizer.get_current_user_id()

    def _track_event(
        self, event_name: str, event_properties: Dict[str, Any], flush: bool = True
    ):

        if not C.ENABLE_USAGE_TRACKING or not C.TRACKING_HOST:
            return
        try:
            user_id = self._get_current_user_id()
            if user_id is None:
                H.LOGGER.debug("Unauthenticated user, tracking disabled")
                return
            event_properties["app_version"] = __version__
            event_properties["environment"] = C.ENVIRONMENT

            analytics.track(
                user_id=user_id, event=event_name, properties=event_properties
            )
            if flush:
                analytics.flush()
        except Exception as exc:
            H.LOGGER.warn(exc)

    def _identify(self, user_properties: Dict[str, Any]):
        if not C.ENABLE_USAGE_TRACKING:
            return
        try:
            user_id = self._get_current_user_id()
            analytics.identify(user_id=user_id, traits=user_properties)
        except Exception as exc:
            H.LOGGER.error(exc)

    def register_new_user(self, email: str, role: str, **kwargs):
        kwargs["email"] = email
        kwargs["role"] = role
        self._identify(user_properties=kwargs)
        self._track_event("user_saved", kwargs)

    def track_page_view(self, request: flask.Request, response: flask.Response):
        if request.path.startswith("/_") or request.path.startswith("/assets/"):
            return

        self._track_event(
            "page_viewed",
            {
                "url": request.url,
                "host_url": request.host_url,
                "response_status_code": response.status_code,
            },
            flush=False,
        )

    def track_connection_saved(self, connection: M.Connection):
        self._track_event(
            "connection_saved",
            {
                "connection_id": connection.id,
                "connection_name": connection.connection_name,
                "catalog": connection.catalog,
                "host": connection.host,
                "connection_type": connection.connection_type.name.lower(),
            },
        )

    def track_project_saved(self, project: M.Project):
        self._track_event(
            "project_saved",
            {
                "project_id": project.id,
                "project_name": project.project_name,
                "connection_id": project.connection.id,
                "table_names": ",".join(
                    [t.get_full_name() for t in project.event_data_tables]
                ),
                "discovery_sample_size": project.discovery_settings.min_property_sample_size,
                "explore_end_date_config": project.webapp_settings.end_date_config.name.lower(),
                "explore_custom_end_date": project.webapp_settings.custom_end_date,
            },
        )

    def track_project_discovered(self, discovered_project: M.DiscoveredProject):
        event_names = discovered_project.get_all_event_names()
        self._track_event(
            "project_discovered",
            {
                "project_id": discovered_project.project.id,
                "project_name": discovered_project.project.project_name,
                "connection_id": discovered_project.project.connection.id,
                "event_count": len(event_names),
                "event_names": ",".join(event_names),
            },
        )

    def track_explore_finished(
        self, metric: M.Metric, duration_seconds: float, from_cache: bool
    ):
        if isinstance(metric, M.SegmentationMetric):
            metrc_type = "segmentation"
            segments = 1
        elif isinstance(metric, M.ConversionMetric):
            metrc_type = "conversion"
            segments = len(metric._conversion._segments)
        elif isinstance(metric, M.RetentionMetric):
            metrc_type = "retention"
            segments = 2
        else:
            metrc_type = "unknown"
            segments = 0
        project = metric.get_project()

        self._track_event(
            "metric_explore_finished",
            {
                "project_id": project.id,
                "project_name": project.project_name,
                "connection_id": project.connection.id,
                "connection_type": project.connection.connection_type.name.lower(),
                "metric_type": metrc_type,
                "segments": segments,
                "serialized": json.dumps(SE.to_dict(metric)),
                "duration_seconds": int(duration_seconds),
                "from_cache": from_cache,
            },
        )
