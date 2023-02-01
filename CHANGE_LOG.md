# Change Log

### v0.2.0 - 2023-01-31 Ada Draginda
#### Deprecations
* `client.update_description` has been deprecated in favor of `client.update_field_descriptions`
or `client.update_dataset_description` as the original function does not work with datasets (only fields).
The new functions have also been changed to operate on editable descriptions instead of descriptions. The
deprecated function will be removed in `v1.0.0`
