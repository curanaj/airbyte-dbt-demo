

      create or replace transient table "AIRBYTE_DATABASE".TEST_NORMALIZATION."NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_PARTITION_669_DATA"  as
      (
-- Final base SQL model
select
    _AIRBYTE_PARTITION_HASHID,
    CURRENCY,
    _airbyte_emitted_at,
    _AIRBYTE_DATA_HASHID
from "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION."NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_PARTITION_669_DATA_AB3"
-- DATA at nested_stream_with_complex_columns_resulting_into_long_names/partition/DATA from "AIRBYTE_DATABASE".TEST_NORMALIZATION."NESTED_STREAM_WITH_COMPLEX_COLUMNS_RESULTING_INTO_LONG_NAMES_64A_PARTITION"
      );
    