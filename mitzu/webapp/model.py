from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

import mitzu.model as M
import mitzu.visualization.common as C


@dataclass(frozen=True)
class SavedMetric:
    """
    SavedMetric class to store a group of a Metric and a SimpleChart

    :param metric: metric
    :param chart: simple chart
    """

    # TODO: introduce id, name here instead of the Metrci itself.
    metric: M.Metric

    # TODO: replace SimpleChart with ABC
    chart: C.SimpleChart
    project: M.Project
    image_base64: str
    small_base64: str
    saved_at: datetime = field(default_factory=datetime.now)


@dataclass()
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

    saved_metric_id: str
    x: int = 0
    y: int = 0
    width: int = 2
    height: int = 8
    saved_metric: Optional[SavedMetric] = None


@dataclass()
class Dashboard:
    """
    Contains all details of a Dashboard.

    """

    name: str
    id: str = field(default_factory=lambda: str(uuid4())[-12:])
    dashboard_metrics: List[DashboardMetric] = field(default_factory=lambda: [])
    created_on: datetime = field(default_factory=lambda: datetime.now())
    last_modified: Optional[datetime] = None
