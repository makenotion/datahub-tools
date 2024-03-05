from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import datahub.emitter.mce_builder as builder
import jmespath
from datahub_tools.dbt import (
    ModelDependencies,
    extract_dbt_resources,
    get_dbt_dependencies,
)


@dataclass
class PriorityMetadata:
    unique_id: str
    resource_name: str
    is_reported_externally: bool
    is_used_to_drive_company_metrics: bool

    original_is_reported_externally: bool = field(init=False)
    original_is_used_to_drive_company_metrics: bool = field(init=False)

    def __post_init__(self):
        self.original_is_reported_externally = self.is_reported_externally
        self.original_is_used_to_drive_company_metrics = (
            self.is_used_to_drive_company_metrics
        )

    @classmethod
    def from_resource(cls, resource: dict) -> PriorityMetadata:
        # note that our metadata is at this path, ['config']['notion_metadata'] but
        # this path is not a standard entry found in dbt resources.
        metadata = jmespath.search("config.notion_metadata", resource) or {}
        table_name = f"{resource['alias'] or resource['name']}"
        resource_name = f"{resource['database']}.{resource['schema']}.{table_name}"
        is_reported_externally = metadata.get("is_reported_externally")
        is_used_to_drive_company_metrics = metadata.get(
            "is_used_to_drive_company_metrics"
        )

        return PriorityMetadata(
            unique_id=resource["unique_id"],
            resource_name=resource_name,
            is_reported_externally=is_reported_externally or False,
            is_used_to_drive_company_metrics=is_used_to_drive_company_metrics or False,
        )

    def propagate(self, other: PriorityMetadata) -> None:
        self.is_reported_externally = (
            self.is_reported_externally or other.is_reported_externally
        )
        self.is_used_to_drive_company_metrics = (
            self.is_used_to_drive_company_metrics
            or other.is_used_to_drive_company_metrics
        )

    def get_priority(self) -> str:
        if self.is_reported_externally:
            out = "P0"
        elif self.is_used_to_drive_company_metrics:
            out = "P1"
        else:
            out = "P2"

        return out

    def get_priority_urn(self) -> str:
        return builder.make_tag_urn(f"Priority: {self.get_priority()}")

    def get_metadata_for_datahub(self) -> dict[str, str]:
        keys = {
            "has downstream dependencies that are reported externally",
            "has downstream dependencies that are used to drive company metrics",
            "is reported externally",
            "is used to drive company metrics",
        }
        vals = {
            self.is_reported_externally,
            self.is_used_to_drive_company_metrics,
            self.original_is_reported_externally,
            self.original_is_used_to_drive_company_metrics,
        }
        return {k: str(v) for k, v in dict(zip(keys, vals)).items()}


def generate_priority_metadata(
    manifest_file: str | Path,
) -> list[PriorityMetadata]:
    """
    For each resource in the given dbt model, find the priority metadata (e.g.
    "is_reported_externally"), propagate it to upstream dependencies and then return a
    list of each resource as a PriorityMetadata object (one object is one dbt resource).
    :param manifest_file: dbt manifest.json generated using `dbt docs`
    """
    # unique_id : resource
    dbt_resources_by_unique_id: dict[str, dict[str, Any]] = extract_dbt_resources(
        manifest_file=manifest_file, resource_type_filter=["model", "snapshot"]
    )
    # unique_id : dependencies
    deps: dict[str, ModelDependencies] = get_dbt_dependencies(
        dbt_resources_by_unique_id=dbt_resources_by_unique_id
    )

    priority_metadata_objs = {
        unique_id: PriorityMetadata.from_resource(r)
        for unique_id, r in dbt_resources_by_unique_id.items()
    }
    # propagate the metadata to all upstream tables. Since we always use 'or' when
    # propagating the boolean flags, we'll always have `true` set for the metadata
    # entries if the table or at least 1 downstream dep is true.
    for unique_id, priority_metadata_obj in priority_metadata_objs.items():
        upstream_unique_ids = deps[unique_id].get_all_upstream()
        for upstream_unique_id in upstream_unique_ids:
            priority_metadata_objs[upstream_unique_id].propagate(priority_metadata_obj)

    return list(priority_metadata_objs.values())
