from __future__ import annotations


import dash_bootstrap_components as dbc
from typing import Any, Dict, Optional

TOAST_INDEX_TYPE = "mitzu_toast"


def create_toast(
    id: str, duration: Optional[int] = 5000, position_style: Dict[str, Any] = None
) -> dbc.Toast:
    if position_style is None:
        position_style = {"top": 66, "right": 10}
    return dbc.Toast(
        children=[],
        id=id,
        is_open=False,
        header=[],
        dismissable=True,
        duration=duration,
        style={
            "position": "fixed",
            "z-index": "1000",
            "width": "fit-content",
            **position_style,
        },
    )
