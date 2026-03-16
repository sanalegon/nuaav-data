{% macro generate_schema_name(custom_schema_name, node) -%}

{#
  generate_schema_name
  --------------------
  Overrides dbt's default schema naming to use the custom schema directly
  (without prefixing the target.schema), which is required for Snowflake
  multi-schema projects where STAGING, PRESENTATION, MONITORING are
  explicit schema names — not suffixes.

  Without this override:
    dev target with schema=STAGING → model ends up in STAGING_STAGING
    or FINANCIAL_DW_DEV.DEV_STAGING etc.

  With this override:
    models with +schema: PRESENTATION → FINANCIAL_DW_DEV.PRESENTATION
    models with +schema: STAGING      → FINANCIAL_DW_DEV.STAGING
    models with no custom schema      → FINANCIAL_DW_DEV.<target.schema>
#}

{%- if custom_schema_name is none -%}
    {{ target.schema | trim }}
{%- else -%}
    {{ custom_schema_name | trim }}
{%- endif -%}

{%- endmacro %}
