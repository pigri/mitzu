from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

import mitzu.model as M
import mitzu.visualization.common as C
import mitzu.serialization as SE


@dataclass(frozen=True, init=False)
class SavedMetric(M.Identifiable):
    """
    SavedMetric class to store a group of a Metric and a SimpleChart

    :param chart: simple chart
    :param small_base64: image in base64 string format to represent the metric as thumbnail
    :param image_base64: larger image in base64 string format to represent the metric
    :param project: the project the metric can be queried on
    :param metric: metric
    :param saved_at: the time of creation
    """

    # TODO: introduce id, name here instead of the Metrci itself.

    chart: C.SimpleChart
    image_base64: str
    small_base64: str
    saved_at: datetime
    _metric_json: str
    _project_ref: M.Reference[M.Project]
    _metric_state: M.State[
        M.Metric
    ]  # We shouldn't pickle the Metric as it won't have references to it's project.

    def __init__(
        self,
        metric: M.Metric,
        chart: C.SimpleChart,
        image_base64: str,
        small_base64: str,
        project: M.Project,
    ):

        object.__setattr__(self, "chart", chart)
        object.__setattr__(self, "image_base64", image_base64)
        object.__setattr__(self, "small_base64", small_base64)
        object.__setattr__(self, "saved_at", datetime.now())
        object.__setattr__(self, "_project_ref", M.Reference(project))
        object.__setattr__(self, "_metric_state", M.State(metric))
        object.__setattr__(self, "_metric_json", SE.to_compressed_string(metric))

    @property
    def project(self) -> M.Project:
        res = self._project_ref.get_value()
        if res is None:
            raise M.InvalidReferenceException(
                "Project is missing from SavedMetric reference"
            )
        return res

    @property
    def metric(self) -> M.Metric:
        res = self._metric_state.get_value()
        if res is None:
            res = SE.from_compressed_string(self._metric_json, self.project)
            self._metric_state.set_value(res)
        return res

    def set_project(self, project: M.Project):
        self._project_ref.set_value(project)

    def get_project_id(self) -> str:
        res = self._project_ref.get_id()
        if res is None:
            raise M.InvalidReferenceException("SavedMetric has no Project ID")
        return res

    def get_id(self) -> str:
        return self.metric._id


@dataclass(init=False)
class DashboardMetric:
    """
    DashboardMetric class to store the positions of a Metric on the Dashboard

    :param saved_metric_id: the id of the corresponding saved_metric
    :param x: X pos
    :param y: Y pos
    :param width: Width
    :param height: Height
    :param saved_metric: The resolved saved_metric
    """

    x: int
    y: int
    width: int
    height: int
    _saved_metric_ref: M.Reference[SavedMetric]

    def __init__(
        self,
        saved_metric: SavedMetric,
        x: int = 0,
        y: int = 0,
        width: int = 2,
        height: int = 8,
    ):
        object.__setattr__(self, "x", x)
        object.__setattr__(self, "y", y)
        object.__setattr__(self, "width", width)
        object.__setattr__(self, "height", height)
        object.__setattr__(self, "_saved_metric_ref", M.Reference(saved_metric))

    @property
    def saved_metric(self) -> SavedMetric:
        res = self._saved_metric_ref.get_value()
        if res is None:
            raise M.InvalidReferenceException(
                "SavedMetric is missing from DashboardMetric reference"
            )
        return res

    def set_saved_metric(self, saved_metric: SavedMetric):
        self._saved_metric_ref.set_value(saved_metric)

    def get_saved_metric_id(self) -> str:
        res = self._saved_metric_ref.get_id()
        if res is None:
            raise M.InvalidReferenceException("DashboardMetric has no SavedMetric ID")
        return res


@dataclass()
class Dashboard:
    """
    Contains all details of a Dashboard.

    param name: the name of the dashboard
    param id: the id of the dashboard
    param dashboard_metric: list of dashboard metrics
    created_on: the time of creation of the dashboard
    """

    name: str
    id: str = field(default_factory=lambda: str(uuid4())[-12:])
    dashboard_metrics: List[DashboardMetric] = field(default_factory=list)
    created_on: datetime = field(default_factory=datetime.now)
    last_modified: Optional[datetime] = None
