# Change Log

### v2.2.1 - 2025-03-12 Peter Ray

- Add group info to get users function

### v2.2.0 - 2024-11-04 Theo Wou

- Add assign role to multiple users, up linter versions

### v2.1.4 - 2024-03-05 Theo Wou

- Support tab, new-line and backslash in escaped characters

### v2.1.3 - 2024-03-05 Theo Wou

- PEP 585 Typing generics

### v2.1.2 - 2024-03-05 Theo Wou

- Rename `_clean_string` to `_escape_quotes` and fix downstream graphql queries

### v2.1.1 - 2023-09-20 Theo Wou

- Fix extraneous braces in `get_datahub_entities`

### v2.1.0 - 2023-07-17 Theo Wou

#### Changes

- `update_field_descriptions` now uses the `updateDescription` endpoint to prevent overwriting
  of tags & glossary terms
- `get_datahub_entities` now accepts an optional `resource_urns` argument to retrieve only the specified urns

### v2.0.1 - 2023-07-20 Ada Draginda

- Fix in `get_datahub_entities`: updated how we fetch the owner type as the underlying API had changed.

### v2.0.0 - 2023-07-12 Ada Draginda

#### Deprecations

- groups and users selection and return have been changed significantly to meet
  the new format by acryl.

#### Changes

- minimum version of acryl-datahub is now 0.10.3.2

### v1.1.0 - 2023-05-23 Ada Draginda

#### Changes

- Added a get_glossary_terms function

#### Fixes

- Fix an incorrect substitution when fetching entities

### v1.0.0 - 2023-02-21 Ada Draginda

#### Deprecations

- `extract_dbt_resources` has moved from a soft to a hard deprecation. Instead, use
  `datahub_tools.dbt.extract_dbt_resources`
- `client.update_descriptions` has moved from a soft to a hard deprecation. Instead, use
  `update_field_descriptions` or `update_dataset_description`

#### Changes

- DataHub posts are now logged with fewer linebreaks and repeated spaces
- `client.get_datahub_entities` no longer returns schema data, by default. You can turn this feature
  back on with the `with_schema` argument. This change was made for performance reasons.

### v0.4.0 - 2023-02-09 Ada Draginda

#### Changes

- Added new `get_owners` to the client module

### v0.3.0 - 2023-02-07 Ada Draginda

#### Changes

- Added a example on how to use transformers
- New DBT module for fetching DBT dependency lineage

### v0.2.0 - 2023-01-31 Ada Draginda

#### Deprecations

- `client.update_description` has been deprecated in favor of `client.update_field_descriptions`
  or `client.update_dataset_description` as the original function does not work with datasets (only fields).
  The new functions have also been changed to operate on editable descriptions instead of descriptions. The
  deprecated function will be removed in `v1.0.0`
