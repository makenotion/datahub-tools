from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import jmespath


class DataHubError(ValueError):
    pass


# generic DH class
@dataclass
class DH:
    name: str
    urn: str


@dataclass
class DHTag(DH):
    def is_dbt(self) -> bool:
        return self.urn.startswith("urn:li:tag:dbt")

    def is_priority(self) -> bool:
        return self.urn.startswith("urn:li:tag:Priority")

    def get_priority(self) -> Optional[str]:
        return self.name.rsplit(" ")[1] if self.is_priority() else None


@dataclass
class DHEntityField:
    name: str
    type: str | None
    description: str | None
    tags: Dict | None
    glossary_terms: Dict | None

    @classmethod
    def from_dict(cls, _dict: Dict) -> DHEntityField:
        # documentation at https://datahubproject.io/docs/graphql/objects#schemafield
        return DHEntityField(
            name=_dict["fieldPath"],
            type=_dict.get("type"),
            description=_dict["description"],
            tags=_dict["tags"],
            glossary_terms=_dict["glossaryTerms"],
        )


@dataclass(frozen=False, unsafe_hash=True)
class DHEntity(DH):
    description: str | None
    editable_description: str | None
    fields: List[DHEntityField]
    editable_fields: List[DHEntityField] | None
    # aka custom properties
    metadata: Dict[str, str] | None
    owners: Dict[str, List[str]] | None
    tags: List[DHTag] | None

    def has_tags(self) -> bool:
        return bool(self.tags)

    def has_owners(self) -> bool:
        return bool(self.owners)

    def get_priority(self) -> Optional[str]:
        """
        return the priority of the dataset, as reflected by its tags (priority tags look like `Priority: P0`)
        :return:
        """
        set_priorities = [x for x in self.tags if x.is_priority()]
        if set_priorities:
            assert len(set_priorities) == 1
            priority_tag = set_priorities[0].get_priority()
        else:
            priority_tag = None

        return priority_tag

    def delete(self, force: bool = True):
        logger = logging.getLogger(__name__)
        logger.info("attempting to delete %s (%s)", self.name, self.urn)
        args = ["datahub", "delete", "--urn", self.urn, "--hard"]
        if force:
            args.append("-f")
        logger.info("sending command: %s", " ".join(args))
        output = subprocess.run(args=args, input="y\n", text=True)
        logger.info("Done: datahub return code %d", output.returncode)

    @classmethod
    def from_dict(cls, _dict: Dict[str, Any]):
        # used to parse the results from query to the graphql search endpoint
        raw_owners = jmespath.search("ownership.owners", _dict) or []
        owner_urns_by_type = {}

        for raw_owner in raw_owners:
            owner_type = raw_owner["type"]
            owner_urns = owner_urns_by_type.get(owner_type, [])
            owner_urns.append(raw_owner["owner"]["urn"])
            if owner_type not in owner_urns_by_type:
                owner_urns_by_type[owner_type] = owner_urns

        # the API docs say that the qualifiedName is the best source for the name
        # and to not use `name`. However, some entities do not have a qualified name,
        # so we need fallback.
        entity_name = (
            jmespath.search("properties.qualifiedName", _dict) or _dict["name"]
        )
        description = jmespath.search("properties.description", _dict)
        editable_description = jmespath.search("editableProperties.description", _dict)

        raw_tags = jmespath.search("tags.tags", _dict) or []
        tags = [
            DHTag(
                urn=tag["tag"]["urn"], name=jmespath.search("tag.properties.name", tag)
            )
            for tag in raw_tags
        ]

        raw_fields = jmespath.search("schemaMetadata.fields", _dict) or []
        fields = [DHEntityField.from_dict(x) for x in raw_fields]

        raw_editable_fields = (
            jmespath.search("editableSchemaMetadata.editableSchemaFieldInfo", _dict)
            or []
        )
        editable_fields = [DHEntityField.from_dict(x) for x in raw_editable_fields]

        raw_metadata = jmespath.search("properties.customProperties", _dict) or []
        raw_metadata = {x["key"]: x["value"] for x in raw_metadata}

        return DHEntity(
            name=entity_name,
            urn=_dict["urn"],
            description=description,
            editable_description=editable_description,
            fields=fields,
            editable_fields=editable_fields,
            metadata=raw_metadata,
            owners=owner_urns_by_type,
            tags=tags,
        )
