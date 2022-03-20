import mitzu.project as P
import mitzu.common.model as M
from datetime import datetime
from mitzu.notebook.model_loader import DatasetModel


def init_project(
    source: M.EventDataSource,
    start_dt: datetime = None,
    end_dt: datetime = None,
    glbs=None,
) -> DatasetModel:
    return P.init_project(source=source, start_dt=start_dt, end_dt=end_dt, glbs=glbs)
