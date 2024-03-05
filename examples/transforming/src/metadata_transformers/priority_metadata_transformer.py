from __future__ import annotations

import logging
import os
from pathlib import Path

import datahub.emitter.mce_builder as builder
from datahub.ingestion.transformer.add_dataset_properties import (
    AddDatasetPropertiesResolverBase,
)
from datahub.metadata.schema_classes import DatasetKeyClass

from metadata_transformers.priority_metadata import (
    PriorityMetadata,
    generate_priority_metadata,
)


class PriorityPropertiesResolver(AddDatasetPropertiesResolverBase):
    def __init__(self):
        super().__init__()
        dbt_target = os.environ.get("DBT_TARGET")
        if not dbt_target:
            raise ValueError(
                "must specify the location of the dbt target files with the "
                "environment variable DBT_TARGET"
            )
        _metadata = generate_priority_metadata(Path(dbt_target) / "manifest.json")
        self.priority_metadata_objs: Dict[str, PriorityMetadata] = {
            x.resource_name: x for x in _metadata
        }

    def get_properties_to_add(self, entity_urn: str) -> Dict[str, str]:
        logger = logging.getLogger(__name__)
        dataset_key: DatasetKeyClass = builder.dataset_urn_to_key(entity_urn)
        priority_metadata_obj: PriorityMetadata = self.priority_metadata_objs.get(
            dataset_key.name
        )
        if priority_metadata_obj:
            priority_properties = priority_metadata_obj.get_metadata_for_datahub()
            logger.info("urn: %s - %s", entity_urn, dataset_key.name)
            logger.info("Adding properties: %s", priority_properties)
        else:
            priority_properties = {}
        return priority_properties
