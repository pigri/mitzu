from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

import mitzu.model as M
import mitzu.visualization.common as C
import mitzu.serialization as SE
from mitzu.helper import create_unique_id


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

    id: str
    name: str
    description: str
    chart: C.SimpleChart
    image_base64: str
    small_base64: str
    created_at: datetime
    last_updated_at: datetime
    owner: Optional[str]
    metric_json: str
    _project_ref: M.Reference[M.Project]
    _metric_state: M.State[
        M.Metric
    ]  # We shouldn't pickle the Metric as it won't have references to it's project.

    def __init__(
        self,
        name: str,
        chart: C.SimpleChart,
        image_base64: str,
        small_base64: str,
        project: M.Project,
        metric_json: Optional[str] = None,
        metric: Optional[M.Metric] = None,
        owner: Optional[str] = None,
        description: Optional[str] = None,
        created_at: Optional[datetime] = None,
        id: Optional[str] = None,
    ):
        if created_at is None:
            created_at = datetime.now()

        if metric_json is None:
            if metric is not None:
                metric_json = SE.to_compressed_string(metric)
            else:
                raise ValueError(
                    "Either metric or metric_json needs to be defined as an argument"
                )

        if id is None:
            id = create_unique_id()
        object.__setattr__(self, "chart", chart)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "description", description if description else "")
        object.__setattr__(self, "image_base64", image_base64)
        object.__setattr__(self, "small_base64", small_base64)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "last_updated_at", datetime.now())
        object.__setattr__(self, "metric_json", metric_json)
        object.__setattr__(self, "owner", owner)
        object.__setattr__(self, "id", id)
        object.__setattr__(self, "_project_ref", M.Reference(project))
        object.__setattr__(self, "_metric_state", M.State(metric))

    @property
    def project(self) -> Optional[M.Project]:
        return self._project_ref.get_value()

    @property
    def metric(self) -> Optional[M.Metric]:
        res = self._metric_state.get_value()
        if self.project is None:
            return None

        if res is None:
            res = SE.from_compressed_string(self.metric_json, self.project)
            self._metric_state.set_value(res)
        return res

    def restore_project(self, project: M.Project):
        self._project_ref.restore_value(project)

    def get_project_id(self) -> str:
        res = self._project_ref.get_id()
        if res is None:
            raise M.InvalidReferenceException("SavedMetric has no Project ID")
        return res

    def get_id(self) -> str:
        return self.id

    def rename(self, name: str) -> SavedMetric:
        if self.project is None:
            raise M.InvalidReferenceException(
                "Renaming saved metric however the project is missing"
            )
        return SavedMetric(
            name=name,
            id=self.id,
            metric_json=self.metric_json,
            description=self.description,
            owner=self.owner,
            chart=self.chart,
            image_base64=self.image_base64,
            small_base64=self.small_base64,
            created_at=self.created_at,
            project=self.project,
        )

    def refresh(
        self,
        image_base64: str,
        image_small64: str,
        metric: Optional[M.Metric] = None,
    ) -> SavedMetric:
        if self.project is None:
            raise M.InvalidReferenceException(
                "Refreshing saved metric however the project is missing"
            )
        return SavedMetric(
            name=self.name,
            id=self.id,
            metric=metric,
            project=self.project,
            description=self.description,
            owner=self.owner,
            chart=self.chart,
            image_base64=image_base64,
            small_base64=image_small64,
            created_at=self.created_at,
        )


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
    def saved_metric(self) -> Optional[SavedMetric]:
        return self._saved_metric_ref.get_value()

    def restore_saved_metric(self, saved_metric: SavedMetric):
        self._saved_metric_ref.restore_value(saved_metric)

    def get_saved_metric_id(self) -> str:
        res = self._saved_metric_ref.get_id()
        if res is None:
            raise M.InvalidReferenceException("DashboardMetric has no SavedMetric ID")
        return res

    def set_saved_metric(self, saved_metric: SavedMetric):
        self._saved_metric_ref.set_value(saved_metric)


@dataclass(frozen=True)
class Dashboard:
    """
    Contains all details of a Dashboard.

    param name: the name of the dashboard
    param id: the id of the dashboard
    param dashboard_metric: list of dashboard metrics
    param created_at: the time of creation of the dashboard
    param owner: the name of the owner
    """

    name: str
    id: str = field(default_factory=lambda: str(uuid4())[-12:])
    dashboard_metrics: List[DashboardMetric] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    last_updated_at: Optional[datetime] = field(default_factory=datetime.now)
    owner: Optional[str] = None

    def update(self, dashboard_metrics: List[DashboardMetric]) -> Dashboard:
        return Dashboard(
            name=self.name,
            id=self.id,
            created_at=self.created_at,
            last_updated_at=datetime.now(),
            dashboard_metrics=dashboard_metrics,
            owner=self.owner,
        )

    def rename(self, name: str) -> Dashboard:
        return Dashboard(
            name=name,
            id=self.id,
            created_at=self.created_at,
            last_updated_at=self.last_updated_at,
            dashboard_metrics=self.dashboard_metrics,
            owner=self.owner,
        )
