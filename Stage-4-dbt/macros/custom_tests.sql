@"
{% macro test_data_volume_threshold(model, column_name, min_rows=100) %}

-- Test that model has minimum number of rows
SELECT
    COUNT(*) AS row_count
FROM {{ model }}
HAVING COUNT(*) < {{ min_rows }}

{% endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
"@ | Out-File -FilePath "macros/custom_tests.sql" -Encoding utf8