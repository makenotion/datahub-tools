# Change Log

### v1.0.0 - 2023-02-21 Ada Draginda
#### Deprecations
* `extract_dbt_resources` has moved from a soft to a hard deprecation. Instead, use
`datahub_tools.dbt.extract_dbt_resources`
* `client.update_descriptions` has moved from a soft to a hard deprecation. Instead, use
`update_field_descriptions` or `update_dataset_description`

#### Changes
* DataHub posts are are now logged with fewer linebreaks and repeated spaces
* `client.get_datahub_entities` no longer returns schema data, by default. You can turn this feature
back on with the `with_schema` argument. This change was made for performance reasons.

### v0.4.0 - 2023-02-09 Ada Draginda
#### Changes
* Added new `get_owners` to the client module

### v0.3.0 - 2023-02-07 Ada Draginda
#### Changes
* Added a example on how to use transformers
* New DBT module for fetching DBT dependency lineage

### v0.2.0 - 2023-01-31 Ada Draginda
#### Deprecations
* `client.update_description` has been deprecated in favor of `client.update_field_descriptions`
or `client.update_dataset_description` as the original function does not work with datasets (only fields).
The new functions have also been changed to operate on editable descriptions instead of descriptions. The
deprecated function will be removed in `v1.0.0`
