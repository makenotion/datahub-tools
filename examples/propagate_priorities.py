"""

This script is a demonstration of an application of this package and was generated as part of a talk at a
DataHub Town Hall. Deck:
https://docs.google.com/presentation/d/1m-97klgCTcMXqacH10vpj2JE6NEWhWp4z5Hh9WhAf2c/edit?usp=sharing

The script takes all existing DataHub entities and insert a priority tag that is calculated based on dbt
metadata. The script works with any data source, but was originally written with DBT resources (dicts come
from the generated manifest.json).

To get the script to run, a user has to fill out two functions (clearly annotated) :
* calculate_priority_tag: For a given dict, return a priority (e.g. P0, P1, P2)
* assemble_priority_metadata: For a given dict, generate metadata that you would like to propagate to DataHub

Author: Ada Draginda
Date: January 25, 2023
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import click
import datahub.emitter.mce_builder as builder
from datahub_tools.classes import DHEntity
from datahub_tools.client import (
    emit_metadata,
    get_datahub_entities,
    remove_tags,
    set_tags,
)
from datahub_tools.utils import extract_dbt_resources


def _to_priority_urn(priority: str) -> str:
    assert len(priority) == 2
    return builder.make_tag_urn(f"Priority: {priority}")


# ----> REQUIRES EDITING
def calculate_priority_tag(dbt_resource: dict[str, Any]) -> str:
    # insert logic for calculating a priority based on the metadata in the dbt resource
    priority = ...
    return builder.make_tag_urn(f"Priority: {priority}")


# ----> REQUIRES EDITING
def assemble_priority_metadata(dbt_resource: dict[str, Any]) -> dict[str, str]:
    """
    Given a dictionary for a given dbt resource (from manifest.json), calculate a
    priority. The return must be a valid URN (see _to_priority_urn which can create an
    example priority tag given a priority such as P0, P1, etc).
    """
    return ...


def _set_priority_tags(
    add_tags: dict[str, list[DHEntity]],
    rem_tags: dict[str, list[DHEntity]],
    dry_run: bool,
):
    logger = logging.getLogger(__name__)
    logger.info("--- Tags to add:")
    for tag, resources in add_tags.items():
        for res in resources:
            logger.info("%s : %s", tag, res.name)
    logger.info("--- Tags to remove:")
    for tag, resources in rem_tags.items():
        for res in resources:
            logger.info("%s : %s", tag, res.name)

    if not dry_run:
        # -- removals
        for tag_urn, entities in rem_tags.items():
            remove_tags(tag_urns=tag_urn, resource_urns=[x.urn for x in entities])
            for entity in entities:
                metadata = entity.metadata.copy()
                if "priority" in metadata:
                    metadata.pop("priority")
                emit_metadata(metadata=metadata, resource_urn=entity.urn)

        # -- adds
        for tag_urn, entities in add_tags.items():
            set_tags(tag_urns=tag_urn, resource_urns=[x.urn for x in entities])
            for entity in entities:
                metadata = entity.metadata.copy()
                # tag_urn example: urn:li:tag:Priority: P0
                # tag_urn.rsplit(" ")[1] ==> 'P0'
                metadata["priority"] = tag_urn.rsplit(" ")[1]
                emit_metadata(metadata=metadata, resource_urn=entity.urn)


@click.command()
@click.option(
    "--manifest_file",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        path_type=Path,
    ),
    help="Manifest file generated from `dbt docs`",
)
@click.option("--dry_run", is_flag=True, default=False)
@click.pass_context
def propagate_dbt_priority_metadata(manifest_file: Path, dry_run: bool):  # noqa
    """
    Propagates dbt priority metadata flags to upstreams dbt resources

    1) Calculates the priority metadata for a table based on its upstream dependencies
    2) uses the metadata to calculate and set a priority (P0, P1, P2)
    3) writes the metadata to datahub
        - The 4 priority entities for the table (e.g. is reported externally)
        - The same 4 but as calculated by upstream dependencies (e.g. has upstream
          dependencies that are reported externally
    """
    logger = logging.getLogger(__name__)

    dbt_resources_by_unique_id = extract_dbt_resources(manifest_file=manifest_file)
    # we need the resources by its storage name instead of its unique_id as that is how
    # DataHub names resources
    dbt_resources_by_name = {
        f"{v['database']}.{v['schema']}.{v['alias'] or v['name']}": v
        for v in dbt_resources_by_unique_id.values()
    }
    entities: list[DHEntity] = get_datahub_entities()

    add_tags = {}
    rem_tags = {}
    for dh_entity in entities:
        dbt_resource = dbt_resources_by_name.get(dh_entity.name)
        if not dbt_resource:
            # the DataHub entity is not a dbt resource
            continue

        new_priority_tag_urn = calculate_priority_tag(dbt_resource)
        existing_priority_tag_urn = _to_priority_urn(dh_entity.get_priority())

        # ------
        # Depending on the new and existing priority tags, add/remove tags as appropriate
        if new_priority_tag_urn:
            if existing_priority_tag_urn:
                if new_priority_tag_urn != existing_priority_tag_urn:
                    entities = rem_tags.get(existing_priority_tag_urn, [])
                    entities.append(dh_entity)
                    if existing_priority_tag_urn not in rem_tags:
                        rem_tags[existing_priority_tag_urn] = entities

                    entities = add_tags.get(new_priority_tag_urn, [])
                    entities.append(dh_entity)
                    if new_priority_tag_urn not in add_tags:
                        add_tags[new_priority_tag_urn] = entities
            else:
                entities = add_tags.get(new_priority_tag_urn, [])
                entities.append(dh_entity)
                if new_priority_tag_urn not in add_tags:
                    add_tags[new_priority_tag_urn] = entities
        elif existing_priority_tag_urn:
            entities = rem_tags.get(existing_priority_tag_urn, [])
            entities.append(dh_entity)
            if existing_priority_tag_urn not in rem_tags:
                rem_tags[existing_priority_tag_urn] = entities
        # ------

        new_priority_metadata = assemble_priority_metadata(dbt_resource)
        metadata = dh_entity.metadata.copy()
        metadata.update(new_priority_metadata)
        if dry_run:
            logger.info(
                "dry run: emit to %s:\n%s\nFull metadata: %s",
                dh_entity.urn,
                new_priority_metadata,
                metadata,
            )
        else:
            emit_metadata(metadata=metadata, resource_urn=dh_entity.urn)

    logger.info("[%d] Tags to add, [%d] tags to remove", len(add_tags), len(rem_tags))

    _set_priority_tags(add_tags=add_tags, rem_tags=rem_tags, dry_run=dry_run)
