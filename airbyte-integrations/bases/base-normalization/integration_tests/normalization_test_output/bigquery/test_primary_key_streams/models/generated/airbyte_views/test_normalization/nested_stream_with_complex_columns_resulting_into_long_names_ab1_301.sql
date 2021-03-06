{{ config(alias="nested_stream_with_complex_columns_resulting_into_long_names_ab1", schema="_airbyte_test_normalization", tags=["top-level-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    {{ json_extract_scalar('_airbyte_data', ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['date']) }} as date,
    {{ json_extract('_airbyte_data', ['partition']) }} as {{ adapter.quote('partition') }},
    _airbyte_emitted_at
from {{ source('test_normalization', '_airbyte_raw_nested_stream_with_complex_columns_resulting_into_long_names') }}
-- nested_stream_with_complex_columns_resulting_into_long_names

