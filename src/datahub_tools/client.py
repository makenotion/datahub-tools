from __future__ import annotations

import functools
import logging
import os
import re
from collections.abc import Iterable
from string import Template
from textwrap import dedent

import jmespath
import requests
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass

from .classes import DataHubError, DHEntity

BUSINESS_OWNER = "urn:li:ownershipType:__system__business_owner"
TECHNICAL_OWNER = "urn:li:ownershipType:__system__technical_owner"


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


def datahub_post(body: dict) -> dict:
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
    # the sub just condenses down the body e.g.: 'query: \n       {...' -> 'query: {...'
    # Note that the extra backslash is needed (\\n+) because body is a dict and calling
    # str will inject additional escape characters.
    logger.info("posting to %s: %s", graphql_url, re.sub(r"\\n+\s*", "", str(body)))

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
    metadata: dict[str, str],
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
            "metadata must be of type dict[str, str] (only strings allowed). Either fix the data, or use "
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


def get_datahub_entities(
    start: int = 0,
    limit: int | None = None,
    with_schema: bool = False,
    chunk_size: int | None = None,
    resource_urns: list[str] | None = None,
) -> list[DHEntity]:
    """
    :param start: Index of the first record to return
    :param limit: Maximum number of records to query
    :param with_schema: If True (default is False) then schema fields and descriptions
      will be retrieved (warning: may be slow or cause the DataHub endpoint to 503, in
      which case you will need to retrieve your entities in chunks).
    :param chunk_size: If provided, the entities will be retrieved in chunks of this
      size.
    :param resource_urns: Optional list of dataset resource_urns
    :return: dictionary of snowflake name (e.g. prep.core.calendar) to DataHub urn
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

    schema_query = Template(
        dedent(
            """
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
        """
        )
    ).substitute(field_vars=field_vars)

    query_fields = Template(
        dedent(
            """
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
                      $schema_query
                      ownership {
                        owners {
                          ownershipType { info { name } }
                          owner {
                            ...on CorpUser { urn editableProperties { email }}
                            ...on CorpGroup { urn editableProperties { email }}
                          }
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

            """
        )
    ).substitute(schema_query=schema_query if with_schema else "")

    _start = start
    _chunk_size = chunk_size or 10000
    _limit = limit or float("inf")

    out: list[DHEntity] = []

    while len(out) < (1 if resource_urns else _limit):
        if resource_urns:
            dataset_query = Template('dataset$i: dataset(urn: "$urn"){ $query_fields }')

            query_parts = [
                dataset_query.substitute(i=i + 1, urn=urn, query_fields=query_fields)
                for i, urn in enumerate(resource_urns)
            ]

            query_body = "{" + ",".join(query_parts) + "}"

        else:
            query_body = Template(
                """
                {
                    search(input: {type: DATASET, query: "*", start: $start, count: $limit}){
                        start
                        count
                        searchResults {
                            entity { $query_fields }
                        }
                    }
                }
                """
            ).substitute(start=_start, limit=_chunk_size, query_fields=query_fields)

        body = {"query": query_body, "variables": {}}

        try:
            query_result = datahub_post(body=body)["data"]
            if resource_urns:
                dh_entities = [{"entity": v} for k, v in query_result.items()]
            else:
                dh_entities = query_result["search"]["searchResults"]
        except DataHubError as e:
            if e.args:
                error_classification = jmespath.search(
                    "errors[0].extensions.classification", e.args[0]
                )
                if error_classification == "DataFetchingException":
                    break
                else:
                    raise e
            else:
                raise e
        else:
            if not dh_entities:
                break
            _start += _chunk_size
            # important: you can get more than one URN per qualified name because there
            # may be more than one platform (e.g. dbt, snowflake, etc.).
            for dh_entity in dh_entities:
                out.append(DHEntity.from_dict(_dict=dh_entity["entity"]))

    return out


def get_owners(
    resource_urn: str, user_fields: str = "urn type", group_fields: str = "urn type"
) -> list[dict[str, str]]:
    """
    Find a list of owners for a given resource_urn. Example format of one entry in the
    output list is{'urn': 'urn:li:corpGroup:my_group_team_name', 'type': 'CORP_GROUP'}
    :param resource_urn: The resource you want to fetch
    :param user_fields: The fields you want to extract from the CorpUser object.
      Defaults to 'urn type'. See
      https://datahubproject.io/docs/graphql/objects#corpuser
    :param group_fields: The fields you want to extract from the CorpUser object.
      Defaults to 'urn type'. See
      https://datahubproject.io/docs/graphql/objects#corpgroup
    :return: A list of dictionaries, each a type of owner for the given resource
    """
    body = {
        "query": (
            Template(
                """
                {
                    dataset(urn: "$resource_urn") {
                        ownership {
                            owners {
                                owner {
                                    ... on CorpUser { $user_fields }
                                    ... on CorpGroup { $group_fields }
                                }
                            }
                        }
                    }
                }
            """
            )
        ).substitute(
            resource_urn=resource_urn,
            user_fields=user_fields,
            group_fields=group_fields,
        ),
        "variables": {},
    }
    response = datahub_post(body=body) or {}
    raw_owners = jmespath.search("data.dataset.ownership.owners", response) or []
    return [x["owner"] for x in raw_owners]


def get_glossary_terms() -> list[dict]:
    body = {
        "query": dedent(
            """
            {
              search(input: {type: GLOSSARY_TERM, query: "*", start:0, count: 1000}) {
                searchResults {
                  entity {
                    urn
                    ... on GlossaryTerm{
                      deprecation { deprecated }
                      properties { name }
                      parentNodes {
                        nodes { urn properties { name } }
                      }
                    }
                  }
                }
              }
            }
            """
        ),
        "variables": {},
    }
    search_results = datahub_post(body=body)["data"]["search"]["searchResults"]
    glossary_terms = []
    for entity in search_results:
        glossary_term = entity["entity"]

        parents = []
        for parent in glossary_term["parentNodes"]["nodes"]:
            parents.append(
                {
                    "urn": parent["urn"],
                    "name": parent["properties"]["name"],
                }
            )

        glossary_terms.append(
            {
                "urn": glossary_term["urn"],
                "is_deprecated": bool(glossary_term["deprecation"]),
                "name": glossary_term["properties"]["name"],
                "parents": parents,
            }
        )
    return glossary_terms


def get_datahub_users() -> list[dict[str, str]]:
    """
    :return: list of datahub users and their metadata (including urn)
    """
    qry = """
    {
        listUsers(input: { query: "*", start: 0, count: 10000 }) {
            users {
                urn
                type
                username
                properties {
                    active
                    displayName
                    email
                    title
                    fullName
                    departmentName
                }
                editableProperties {
                    displayName
                    title
                    teams
                    slack
                    email
                }
                status
                isNativeUser
                relationships(input: {
                    types: ["IsMemberOfNativeGroup"],
                    direction: OUTGOING,
                    start: 0,
                    count: 1000
                }) {
                    relationships {
                        entity {
                          ...on CorpGroup {
                            urn
                            type
                            properties { displayName }
                            editableProperties {
                                email
                                slack
                            }
                          }
                        }
                    }
                }
            }
        }
    }
    """

    body = {
        "query": qry,
        "variables": {},
    }
    users = []
    for user in datahub_post(body=body)["data"]["listUsers"]["users"]:
        # editable_props = user.pop("editableProperties")
        # if editable_props:
        #     user.update(editable_props)
        users.append(user)
    return users


def get_datahub_groups() -> list[dict[str, str]]:
    """
    :return: list of datahub groups and their metadata (including urn)
    """
    qry = """
    {
        listGroups(input: { query: "*", start: 0, count: 10000 }) {
            groups {
                urn
                type
                name
                properties {
                    displayName
                    description
                    email
                    slack
                }
                editableProperties {
                    description
                    slack
                    email
                }
            }
        }
    }
    """

    body = {
        "query": qry,
        "variables": {},
    }
    users = []
    for user in datahub_post(body=body)["data"]["listGroups"]["groups"]:
        # editable_props = user.pop("editableProperties")
        # if editable_props:
        #     user.update(editable_props)
        users.append(user)
    return users


def _replace_match(match: re.Match) -> str:
    """
    Replace an escaped character match to be compatible with the query template
    """
    _replace_map = {
        '"': '\\"',
        "\n": "\\n",
        "\t": "\\t",
    }
    return _replace_map.get(match.group(0))


def _escape_chars(_str: str) -> str:
    """
    Convenience method to handle escaped characters within a string
    For GraphQL queries this cannot be used within an f-string otherwise it will incorrectly escape the quotes
        e.g. f'"{_escape_chars('\"foo\"}"' will output '\\\"foo\\\"' instead of the desired '\\"foo\\"'
        instead use '"' + _escape_chars('\"foo\") + '"'
    """
    # first escape backslashes separately to prevent other characters from being double escaped
    _backslash_str = re.sub(r"\\(?!n|t)", r"\\\\", _str)

    pattern = r'(["\n\t])'
    return re.sub(pattern, _replace_match, _backslash_str)


def update_field_descriptions(
    resource_urn: str, field_descriptions: dict[str, str]
) -> dict[str, str]:
    """
    Update the editable schema field description for one or more fields within a dataset
    :param resource_urn: The URN for the related dataset/resource
    :param field_descriptions: A dictionary where the key-value pairs are the flattened fieldPath (name)
      for the column and the description.
    :return: Resource URN changed
    """
    responses = {}
    for k, v in field_descriptions.items():
        _input = (
            '{ description: "' + _escape_chars(v) + '", '
            f'resourceUrn: "{resource_urn}", '
            f"subResourceType: DATASET_FIELD, "
            f'subResource: "{k}" }}'
        )
        endpoint = "updateDescription"
        response = _post_mutation(
            endpoint=endpoint,
            _input=_input,
        )
        if not response:
            raise ValueError(
                f"Failed to update field description '{k}' (but returned 200) for {resource_urn}"
            )
        responses[k] = response[endpoint]
    return responses


def update_dataset_description(resource_urn: str, description: str) -> dict[str, str]:
    """
    Update the editable description for a given dataset/resource
    :param resource_urn: The URN for the related dataset/resource
    :param description: The description that you want to set for the dataset/resource
    :return: Resource URN changed
    """
    _input = (
        '{ editableProperties: { description: "' + _escape_chars(description) + '" } }'
    )
    endpoint = "updateDataset"
    response = _post_mutation(
        endpoint=endpoint, _input=_input, urn=resource_urn, subselection="urn"
    )
    if not response:
        raise ValueError(
            f"Failed to update entity descriptions (but returned 200) for {resource_urn}"
        )
    return response[endpoint]


def update_institutional_memory(
    resource_urn: str, url: str, description: str, author_urn: str, created_at: int
):
    """
    An institutional memory to add to a Metadata Entity
    :param resource_urn: URN of the resource/entity that you're trying to set
    :param url: Link to a document or wiki page or another internal resource
    :param description: Description of the resource
    :param author_urn: The corp user urn of the author of the metadata
    :param created_at: The time at which this metadata was created
    :return:
    """
    element = (
        f'{{ url: "{url}", '
        'description: "' + _escape_chars(description) + '", '
        f'author: "{author_urn}", createdAt: {created_at} }}'
    )
    _input = f"{{ institutionalMemory: {{ elements: [{element}] }} }}"
    endpoint = "updateDataset"
    response = _post_mutation(
        endpoint=endpoint, _input=_input, urn=resource_urn, subselection="urn"
    )
    if not response:
        raise ValueError(
            f"Failed to update institutional memory (but returned 200) for {resource_urn}"
        )
    return response[endpoint]


def set_group_owner(
    group_urn: str, resource_urns: list[str], owner_type: str = TECHNICAL_OWNER
):
    owner = f'{{ ownerUrn: "{group_urn}", ownerEntityType: CORP_GROUP, ownershipTypeUrn: "{owner_type}" }}'
    _set_owner(urns=resource_urns, owner=owner)


def set_user_owner(
    user_urn: str, resource_urns: list[str], owner_type: str = BUSINESS_OWNER
):
    owner = f'{{ ownerUrn: "{user_urn}", ownerEntityType: CORP_USER, ownershipTypeUrn: "{owner_type}" }}'
    _set_owner(urns=resource_urns, owner=owner)


def _set_owner(owner: str, urns: list[str]):
    resource_urns = ", ".join([f'{{ resourceUrn: "{urn}" }}' for urn in urns])
    _input = f"{{ owners: [ {owner} ], resources: [ {resource_urns} ] }}"
    response = _post_mutation(endpoint="batchAddOwners", _input=_input)
    if not response:
        raise ValueError(f"Setting table owners for {owner} failed! (but returned 200)")


def remove_owners(owners: Iterable[str], urns: list[str]):
    if isinstance(owners, str):
        owners = [owners]
    owner_urns = ", ".join(f'"{owner}"' for owner in owners)
    resource_urns = ", ".join([f'{{ resourceUrn: "{urn}" }}' for urn in urns])
    _input = f"{{ ownerUrns: [ {owner_urns} ], resources: [ {resource_urns} ] }}"
    response = _post_mutation(endpoint="batchRemoveOwners", _input=_input)
    if not response:
        raise ValueError(
            f"Removing table owners ({owners}) for {urns} failed! (but returned 200)"
        )


def assign_role_to_users(role_urn: str, urns: list[str]):
    _role_urn = f'"{role_urn}"'
    user_urns = ", ".join(f'"{user}"' for user in urns)
    _input = f"{{ roleUrn: {_role_urn}, actors: [ {user_urns} ] }}"
    response = _post_mutation(endpoint="batchAssignRole", _input=_input)
    if not response:
        raise ValueError(
            f"Setting role {role_urn} to users {user_urns} failed! (but returned 200)"
        )


def set_tags(tag_urns: Iterable[str], resource_urns: Iterable[str]):
    _change_tags(
        endpoint="batchAddTags", tag_urns=tag_urns, resource_urns=resource_urns
    )


def remove_tags(tag_urns: Iterable[str], resource_urns: Iterable[str]):
    _change_tags(
        endpoint="batchRemoveTags", tag_urns=tag_urns, resource_urns=resource_urns
    )


def _change_tags(endpoint: str, tag_urns: Iterable[str], resource_urns: Iterable[str]):
    if isinstance(tag_urns, str):
        tag_urns = [tag_urns]
    _tags = ", ".join(f'"{t}"' for t in tag_urns)

    if isinstance(resource_urns, str):
        resource_urns = [resource_urns]
    _res = ", ".join([f'{{ resourceUrn: "{urn}" }}' for urn in resource_urns])
    _input = f"{{ tagUrns: [ {_tags} ], resources: [ {_res} ] }}"

    response = _post_mutation(endpoint=endpoint, _input=_input)
    if not response:
        raise ValueError(
            f"{endpoint} {tag_urns} for {resource_urns} failed! (but returned 200)"
        )


def _post_mutation(
    endpoint: str, _input: str, urn: str | None = None, subselection: str | None = None
) -> dict | None:
    _urn = f'urn: "{urn}", ' if urn else ""
    _subselection = f" {{ {subselection} }}" if subselection else ""
    body = {
        "query": f"mutation {endpoint} {{ {endpoint}({_urn}input: {_input}){_subselection} }}",
        "variables": {},
    }
    response = datahub_post(body=body)
    return response["data"] if response else None
