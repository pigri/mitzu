from mitzu.notebook.model_loader import DatasetModel
import mitzu.common.model as M
from datetime import datetime, timedelta
from typing import Dict, Optional


def init_project(
    source: M.EventDataSource,
    start_dt: datetime = None,
    end_dt: datetime = None,
    glbs: Optional[Dict] = None,
) -> DatasetModel:
    if start_dt is None:
        start_dt = datetime.now() - timedelta(days=365 * 5)
    if end_dt is None:
        end_dt = datetime.now()
    m = source.discover_datasource(
        start_dt=start_dt, end_dt=end_dt
    ).create_notebook_class_model()
    if glbs is not None:
        m._to_globals(glbs)
    return m
