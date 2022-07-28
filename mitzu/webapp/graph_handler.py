from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import mitzu.model as M
from dash import dcc
from mitzu.serialization import to_dict

GRAPH = "graph"

CACHE: Dict[str, Any] = {}


def create_graph(metric: Optional[M.Metric]) -> dcc.Graph:
    if metric is not None:
        key = json.dumps(to_dict(metric))
        fig = CACHE.get(key)
        if fig is None:
            fig = metric.get_figure()
            CACHE[key] = fig
    else:
        fig = {}

    return dcc.Graph(
        id=GRAPH,
        figure=fig,
        config={"displayModeBar": False},
    )


@dataclass
class GraphHandler:

    component: dcc.Graph

    @classmethod
    def from_metric(cls, metric: Optional[M.Metric]) -> GraphHandler:
        figure = {"data": []} if metric is None else metric.get_figure()

        graph = dcc.Graph(
            id=GRAPH,
            className=GRAPH,
            figure=figure,
            config={"displayModeBar": False},
        )
        return GraphHandler(graph)
