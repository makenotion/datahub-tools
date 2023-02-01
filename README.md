# DataHub-Tools

A python quasi-client (and tools) that interacts with DataHub GraphQL endpoints.

## Install

Requires python >=3.7

```bash
pip install https://github.com/makenotion/datahub-tools
```

Three environment variables are required:

* DATAHUB_GMS_URL - e.g. "https://your_business.acryl.io/gms"
* DATAHUB_GRAPHQL_URL - e.g. "https://your_business.acryl.io/api/graphql"
* DATAHUB_GMS_TOKEN - Access token generated from https://your_business.acryl.io/settings/tokens

## Quick-Start

Make sure you have your environment variables set (see above)

_Sample some of your DataHub entities_
```python
from typing import List
from datahub_tools.client import get_datahub_entities
from datahub_tools.classes import DHEntity

entities: List[DHEntity] = get_datahub_entities(limit=5)
```

_Take the first entity and apply some changes_
```python
from datahub_tools.client import update_dataset_description, emit_metadata
entity = entities[0]

# update the table's description
update_dataset_description(
    resource_urn=entity.urn,
    description="The new description for this entity"
)

# update the description for a column within the table
update_field_descriptions(
    resource_urn=entity.urn,
    field_descriptions={"my_column": "A new description for this column"}
)

# set the custom properties
emit_metadata(metadata={'foo': 'bar'}, resource_urn=entity.urn)
```

## Discussion

This package was not created with the intent of open-sourcing (rather just for internal use at Notion) and so
it does not contain wrappers around everything endpoint. There is a lot missing before reaching feature parity
but hopefully still helpful.

The goals of the functions were to provide simple and easy programmatic access to common DataHub operations

#### Features
* emit metadata
* fetch entities (datasets)
* fetch datahub users
* fetch datahub groups
* remove/set user/group owners
* remove/set tags

Users are encouraged to setup logging in advance as many steps and communications are logged to INFO.

## Development/Contributing

Contributions are welcome, but please add tests for new code. Testing is setup using pytest and orchestrated
with tox:

```bash
pyenv shell 3.7.16 3.8.16 3.9.16 3.10.9 3.11.1
pip install -U pip setuptools tox
tox
pyenv shell -
```

### Set up the pre-commit hooks

```bash
brew install pre-commit
pre-commit install
```

The pre-commit hooks will run automatically upon a commit

### Contact/Author

Written by [Ada Draginda](https://www.linkedin.com/in/adadraginda/) <ada[at]makenotion.com>
