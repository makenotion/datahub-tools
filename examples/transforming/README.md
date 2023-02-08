# DBT Ingestion with Transformation Example

This example shows how you can use a metadata transformer to modify metadata during the ingestion of
dbt resources.

This code was written to support a blog post (insert link once published)

# Instructions

1. Add the following bit to your dbt.dhub.yml (see the
[official docs](https://datahubproject.io/docs/generated/ingestion/sources/dbt/))

```yaml
transformers:
    - type: "add_dataset_properties"
      config:
          add_properties_resolver_class: "metadata_transformers.priority_metadata_transformer:PriorityPropertiesResolver"
```

2. Add the environment variable `DBT_TARGET` that directs the tools where it can find your generated `manifest.json`

3. Ingest your data as usual (with `datahub ingest -c dbt.dhub.yaml`)
