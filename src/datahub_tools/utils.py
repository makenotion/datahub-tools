from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def extract_dbt_models(manifest_file: str | Path) -> Dict[str, Dict[str, Any]]:
    """
    :return: A dictionary containing the snowflake table name (e.g. prep.core.calendar)
      and the associated dbt manifest dict (table metadata).
    """
    if isinstance(manifest_file, str):
        manifest_file = Path(manifest_file)
    with manifest_file.open() as f:
        manifest = json.load(f)
        manifest_nodes = manifest["nodes"]

    return {
        f'{data["database"]}.{data["schema"]}.{data["alias"] or data["name"]}': data
        for data in manifest_nodes.values()
        if data["resource_type"] in ["model", "snapshot"]
    }
