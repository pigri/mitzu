from mitzu.notebook.model_loader import ModelLoader, DatasetModel
import mitzu.common.model as M
from mitzu.discovery.dataset_discovery import EventDatasetDiscovery
from datetime import datetime, timedelta
import mitzu.adapters.adapter_factory as AF
from typing import Dict


def init_project(
    source: M.EventDataSource,
    start_dt: datetime = None,
    end_dt: datetime = None,
    glbs: Dict = None,
) -> DatasetModel:
    if start_dt is None:
        start_dt = datetime.now() - timedelta(days=365 * 5)
    if end_dt is None:
        end_dt = datetime.now()

    adapter = AF.get_or_create_adapter(source)
    discovery = EventDatasetDiscovery(source, adapter, start_dt, end_dt)
    dd = discovery.discover_dataset()
    m = ModelLoader().create_dataset_model(dd)
    if globals is not None:
        m._to_globals(glbs)
    return m
