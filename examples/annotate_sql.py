"""

This script is a demonstration of an application of this package and was generated as part of a talk at a
DataHub Town Hall. Deck:
https://docs.google.com/presentation/d/1m-97klgCTcMXqacH10vpj2JE6NEWhWp4z5Hh9WhAf2c/edit?usp=sharing

This script reads your DBT manifest.json and adds a link to its appropriate DataHub page at the top of the
SQL file.

run with
```
python annotate_sql --manifest_file /path/to/your/manifest.json
```

Author: Ada Draginda <ada[at]makenotion.com>
Date: January 25, 2023
"""
from __future__ import annotations

import logging
import pathlib
from logging.config import dictConfig
from string import Template
from typing import Dict, List

import click
from datahub_tools.utils import extract_dbt_resources

# Every link shares this same prefix. We specify it separately so that we can remove
# links before writing them again (avoiding more than one link showing).
# The reason we don't use the full DH_LINK is to support db/schema changes (e.g.
# if we move a table)
DH_LINK_PREFIX = "-- https://notion.acryl.io/dataset/urn:li:dataset:"
DH_LINK = Template(
    f"{DH_LINK_PREFIX}(urn:li:dataPlatform:dbt,$table_name,PROD)  --noqa\n"
)


def setup_logging(level: int = logging.INFO):
    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {
                "format": "[%(asctime)s][%(levelname)7s][ %(name)s ]: %(message)s"
            },
        },
        "handlers": {
            "stdout": {
                "level": level,
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {  # root logger
                "handlers": ["stdout"],
                "level": level,
                "propagate": False,
            },
        },
    }
    dictConfig(config)


def config_to_str(config: Dict):
    all_str = []
    for k, v in config.items():
        if isinstance(v, dict):
            _dict = ",\n            ".join([f"{_k!r}: {_v!r}" for _k, _v in v.items()])
            to_append = f"{k} = {{\n            {_dict}\n        }}"
        elif isinstance(v, bool):
            to_append = f"{k} = {str(v).lower()}"
        else:
            to_append = f"{k} = {v!r}"
        all_str.append(to_append)
    joined_str = ",\n        ".join(all_str)
    out = f"{{{{\n    config(\n        {joined_str}\n    )\n}}}}\n" if config else ""
    return (
        # HACK: see `get_cleaned_config` for why this bit is needed
        out.replace(
            "anomalo_check_offset = \"-var('mrr_look_back_days')\"",
            "anomalo_check_offset = -var('mrr_look_back_days')",
        )
        # True/False works just fine, but we should be consistent across all files and
        # lowercase is used much more often than the upper case flavors.
        .replace(": False", ": false").replace(": True", ": true")
    )


@click.command()
@click.option(
    "--manifest_file",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        path_type=pathlib.Path,
    ),
    help="Manifest file generated from `dbt docs --target datahub`",
)
@click.option(
    "--resource_type_filter",
    multiple=True,
    default=None,
    help="One or more resource_types can be included to filter dbt resources, e.g. 'model', 'snapshot', etc.",
)
def main(manifest_file: pathlib.Path, resource_type_filter: List[str] | None):
    """
    Insert a link to each model's DataHub page at the top of each SQL file
    """
    logger = logging.getLogger(__name__)
    dbt_resources = extract_dbt_resources(
        manifest_file=manifest_file, resource_type_filter=resource_type_filter
    )
    for unique_id, node in dbt_resources:
        logger.info(unique_id)
        original_file_path = node.get("original_file_path")
        if original_file_path and original_file_path.endswith(".sql"):
            # assemble the DataHub link
            name = f"{node['alias'] or node['name']}"
            table_name = f"{node['database']}.{node['schema']}.{name}"
            datahub_link = DH_LINK.substitute(table_name=table_name)
            logger.info(datahub_link)
            code: str = node.get("raw_code") or node.get("raw_sql")

            if not code:
                logger.error("DBT did not produce code for: %s", original_file_path)
                continue

            # remove any existing links
            if code.startswith(DH_LINK_PREFIX):
                code = "\n".join(code.split("\n")[1:])

            # the snapshot header and footer will not be present in the raw sql, and so we have to add them
            # back in before writing
            if node["resource_type"] == "snapshot":
                wrapped_out_code = f"{{% snapshot {name} %}}\n\n{code.strip()}\n\n{{% endsnapshot %}}\n"
            else:
                wrapped_out_code = code

            suffix = "" if wrapped_out_code.endswith("\n") else "\n"

            file_path = pathlib.Path(node["root_path"]) / pathlib.Path(
                original_file_path
            )
            logger.info(file_path)
            with file_path.open(mode="w") as f:
                f.writelines(f"{datahub_link}{wrapped_out_code}{suffix}")


if __name__ == "__main__":
    setup_logging()
    main()
