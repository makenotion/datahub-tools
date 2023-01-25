from __future__ import annotations

import functools
import logging
import os
from string import Template
from textwrap import dedent
from typing import Dict, Iterable, List

import requests
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass

from .classes import DataHubError, DHEntity

BUSINESS_OWNER = "BUSINESS_OWNER"
TECHNICAL_OWNER = "TECHNICAL_OWNER"


def _wrapped_getenv(token) -> str:
    out = os.getenv(token)
    if not out:
        raise ValueError(f"missing environment variable {token}")
    return out


def get_dh_token() -> str:
    return _wrapped_getenv("DATAHUB_GMS_TOKEN")


def get_dh_server() -> str:
    return _wrapped_getenv("DATAHUB_GMS_URL")


def get_dh_graphql_url() -> str:
    return _wrapped_getenv("DATAHUB_GRAPHQL_URL")


def datahub_post(body: Dict) -> Dict:
    """
    Convenience function for sending POSTs to DataHub's GraphQL endpoint
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_dh_token()}",
    }

    graphql_url = get_dh_graphql_url()
    logger = logging.getLogger(__name__)
    logger.info("posting to %s: %s", graphql_url, body)

    response = requests.post(url=graphql_url, headers=headers, json=body)
    response.raise_for_status()
    out = response.json()

    if "errors" in out:
        raise DataHubError(out)
    else:
        return out


@functools.lru_cache(maxsize=None)
def get_dh_emitter():
    return DatahubRestEmitter(
        gms_server=get_dh_server(),
        extra_headers={"Authorization": f"Bearer {get_dh_token()}"},
    )


def emit_metadata(
    metadata: Dict[str, str],
    resource_urn: str,
    cast_to_str: bool = False,
    sort: bool = True,
):
    """
    Using the DataHub emitter included in its package, emit metadata for a given resource/entity.
    :param metadata:
    :param resource_urn:
    :param cast_to_str: Metadata keys and values must all be strings. This argument was added to easily
      coerce your keys/values into strings before they are sent to DataHub.
    :param sort: Metadata are sorted by key, by default
    :return:
    """
    logger = logging.getLogger(__name__)
    logger.info("Emitting metadata %s to table %s", metadata, resource_urn)

    emitter = get_dh_emitter()

    _metadata = (
        {str(k): str(v) for k, v in metadata.items()} if cast_to_str else metadata
    )

    if not all(isinstance(k, str) and isinstance(v, str) for k, v in _metadata.items()):
        raise ValueError(
            "metadata must be of type Dict[str, str] (only strings allowed). Either fix the data, or use "
            "the arg cast_to_str=True to str() wrap all keys and value."
        )

    # noinspection PyTypeChecker
    custom_properties = (
        dict(sorted(_metadata.items(), key=lambda x: x[0])) if sort else _metadata
    )

    metadata_event = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=resource_urn,
        aspectName="datasetProperties",
        aspect=DatasetPropertiesClass(customProperties=custom_properties),
    )

    emitter.emit(metadata_event)


def get_datahub_entities(start: int = 0, limit: int = 10000) -> List[DHEntity]:
    """
    :param start: Index of the first record to return
    :param limit:  Maximum number of records to return (default and maximum per query is 10000).
    :return: Dictionary of snowflake name (e.g. prep.core.calendar) to DataHub urn
      e.g. urn:li:dataset:(urn:li:dataPlatform:snowflake,prep.core.calendar,PROD)
    """
    # reuse the same set of vars for fields (editable and non-editable fields)
    field_vars = dedent(
        """
        fieldPath
        description
        tags {
          tags {
            tag {
              ...on Tag {
                urn
                properties {
                  ...on TagProperties { name }
                }
              }
            }
          }
        }
        glossaryTerms {
          terms {
            term {
              ...on GlossaryTerm {
                urn
                properties {
                  ...on GlossaryTermProperties { name }
                }
              }
            }
          }
        }
    """
    )

    query_body = Template(
        dedent(
            """{
          search(input: {type: DATASET, query: "*", start:$start, count: $limit})
          {
            start
            count
            searchResults {
              entity {
                urn
                type
                ...on Dataset {
                  name
                  properties {
                    qualifiedName
                    description
                    customProperties {
                      ...on CustomPropertiesEntry { key value }
                    }
                  }
                  editableProperties { description }
                  schemaMetadata {
                    fields {
                      ...on SchemaField {
                        $field_vars
                        type
                      }
                    }
                  }
                  editableSchemaMetadata {
                    editableSchemaFieldInfo {
                      ...on EditableSchemaFieldInfo {
                        $field_vars
                      }
                    }
                  }
                  ownership {
                    owners {
                      owner {
                        ...on CorpUser { urn }
                        ...on CorpGroup { urn }
                      }
                      type
                    }
                  }
                  tags {
                    tags {
                      tag {
                        ...on Tag {
                          urn
                          properties {
                            ...on TagProperties { name }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }"""
        )
    ).substitute(field_vars=field_vars, start=start, limit=max(limit, 10000))

    body = {"query": query_body, "variables": {}}
    dh_entities = datahub_post(body=body)["data"]["search"]["searchResults"]

    # important: you can get more than one URN per qualified name because there may be
    # more than one platform (e.g. dbt, snowflake, etc.).
    out: List[DHEntity] = []
    for dh_entity in dh_entities:
        out.append(DHEntity.from_dict(_dict=dh_entity["entity"]))

    return out


def get_datahub_users() -> List[Dict[str, str]]:
    """
    :return: List of datahub users and their metadata (including urn)
    """
    body = {
        "query": (
            '{ listUsers(input: { query: "*", start: 0, count: 10000 }) '
            "{ start count total users "
            "{ ... on CorpUser { urn type username editableProperties "
            "{ ... on CorpUserEditableProperties { displayName email } "
            "} } } } }"
        ),
        "variables": {},
    }
    users = []
    for user in datahub_post(body=body)["data"]["listUsers"]["users"]:
        editable_props = user.pop("editableProperties")
        if editable_props:
            user.update(editable_props)
        users.append(user)
    return users


def get_datahub_groups() -> List[Dict[str, str]]:
    """
    :return: List of datahub group and their metadata (including urn)
    """
    body = {
        "query": (
            '{ listGroups(input: { query: "*", start: 0, count: 10000 }) '
            "{ start count total groups { ... on CorpGroup { urn type name } } } }"
        ),
        "variables": {},
    }
    return datahub_post(body=body)["data"]["listGroups"]["groups"]


def update_description(
    resource_urn: str, description: str, column: str | None = None
) -> bool:
    """
    :param resource_urn: The entity/resource URN to update
    :param description: The new description
    :param column: If left out, then the description is updated for the entity. If provided, then the
      description will be applied to this column (field).
    :return:
    """
    subresource = (
        f', subResourceType: DATASET_FIELD, subResource: "{column}"' if column else ""
    )
    body = {
        "query": (
            "mutation updateDescription { updateDescription(input: {"
            f'resourceUrn: "{resource_urn}", description: "{description}"{subresource}'
            "}) }"
        ),
        "variables": {},
    }
    return datahub_post(body=body)["data"]["updateDescription"]


def set_group_owner(
    group_urn: str, resource_urns: List[str], owner_type: str = TECHNICAL_OWNER
):
    owner = f'{{ ownerUrn: "{group_urn}", ownerEntityType: CORP_GROUP, type: {owner_type} }}'
    _set_owner(urns=resource_urns, owner=owner)


def set_user_owner(
    user_urn: str, resource_urns: List[str], owner_type: str = BUSINESS_OWNER
):
    owner = (
        f'{{ ownerUrn: "{user_urn}", ownerEntityType: CORP_USER, type: {owner_type} }}'
    )
    _set_owner(urns=resource_urns, owner=owner)


def _set_owner(owner: str, urns: List[str]):
    resource_urns = ", ".join([f'{{ resourceUrn: "{urn}" }}' for urn in urns])
    body = {
        "query": (
            "mutation batchAddOwners { batchAddOwners(input: "
            f"{{ owners: [ {owner} ], resources: [ {resource_urns} ] }}) }}"
        ),
        "variables": {},
    }
    response = datahub_post(body=body)
    if not response:
        raise ValueError(f"Setting table owners for {owner} failed! (but returned 200)")


def remove_owners(owners: Iterable[str], urns: List[str]):
    if isinstance(owners, str):
        owners = [owners]
    owner_urns = ", ".join(owners)
    resource_urns = ", ".join([f'{{ resourceUrn: "{urn}" }}' for urn in urns])
    body = {
        "query": (
            "mutation batchRemoveOwners { batchRemoveOwners(input: "
            f"{{ ownerUrns: [ {owner_urns} ], resources: [ {resource_urns} ] }}) }}"
        ),
        "variables": {},
    }
    response = datahub_post(body=body)
    if not response:
        raise ValueError(
            f"Removing table owners ({owners}) for {urns} failed! (but returned 200)"
        )


def set_tags(tag_urns: Iterable[str], resource_urns: Iterable[str]):
    _change_tags(command="batchAddTags", tag_urns=tag_urns, resource_urns=resource_urns)


def remove_tags(tag_urns: Iterable[str], resource_urns: Iterable[str]):
    _change_tags(
        command="batchRemoveTags", tag_urns=tag_urns, resource_urns=resource_urns
    )


def _change_tags(command: str, tag_urns: Iterable[str], resource_urns: Iterable[str]):
    if isinstance(tag_urns, str):
        tag_urns = [tag_urns]
    _tags = ", ".join(f'"{t}"' for t in tag_urns)

    if isinstance(resource_urns, str):
        resource_urns = [resource_urns]
    _res = ", ".join([f'{{ resourceUrn: "{urn}" }}' for urn in resource_urns])

    body = {
        "query": (
            f"mutation {command} {{ {command}(input: "
            f"{{ tagUrns: [ {_tags} ], resources: [ {_res} ] }}) }}"
        ),
        "variables": {},
    }
    response = datahub_post(body=body)
    if not response:
        raise ValueError(
            f"{command} {tag_urns} for {resource_urns} failed! (but returned 200)"
        )
